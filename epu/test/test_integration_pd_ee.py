# Copyright 2013 University of Chicago

import os
import time
import uuid
import unittest
import logging

from nose.plugins.skip import SkipTest

try:
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")


log = logging.getLogger(__name__)

default_user = 'default'

deployment_one_pd_two_eea = """
process-dispatchers:
  pd_0:
    config:
      processdispatcher:
        engines:
          default:
            deployable_type: eeagent
            slots: 100
            base_need: 2
nodes:
  nodeone:
    dt: eeagent
    process-dispatcher: pd_0
    eeagents:
      eeagent_nodeone:
        heartbeat: 1
        slots: 100
        launch_type: supd
        logfile: /tmp/eeagent_nodeone.log
"""


fake_credentials = {
    'access_key': 'xxx',
    'secret_key': 'xxx',
    'key_name': 'ooi'
}

dt_name = "example"
example_dt = {
    'mappings': {
        'real-site': {
            'iaas_image': 'r2-worker',
            'iaas_allocation': 'm1.large',
        },
        'ec2-fake': {
            'iaas_image': 'xxami-fake',
            'iaas_allocation': 't1.micro',
        }
    },
    'contextualization': {
        'method': 'chef-solo',
        'chef_config': {}
    }
}

example_definition = {
    'general': {
        'engine_class': 'epu.decisionengine.impls.simplest.SimplestEngine',
    },
    'health': {
        'monitor_health': False
    }
}

example_domain = {
    'engine_conf': {
        'preserve_n': 0,
        'epuworker_type': dt_name,
        'force_site': 'ec2-fake'
    }
}

dt_name2 = "with-userdata"
example_userdata = 'Hello Cloudy World'
example_dt2 = {
    'mappings': {
        'ec2-fake': {
            'iaas_image': 'ami-fake',
            'iaas_allocation': 't1.micro',
        }
    },
    'contextualization': {
        'method': 'userdata',
        'userdata': example_userdata
    }
}


class TestIntegrationPDEEAgent(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.deployment = deployment_one_pd_two_eea

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.sysname = "testsysname-%s" % str(uuid.uuid4())
        self.user = default_user

        self.setup_harness(exchange=self.exchange, sysname=self.sysname)
        self.addCleanup(self.cleanup_harness)

        # Set up fake libcloud and start deployment
        self.site_name = "ec2-fake"
        self.fake_site, self.libcloud = self.make_fake_libcloud_site(self.site_name)

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.pd_client = clients['pd_0']

        self.block_until_ready(self.deployment, self.dashi)

    def test_example(self):

        definition_id = "test_definition"
        definition_type = "supd"
        name = definition_id
        description = "Some Process"

        executable = {
            'exec': '/bin/sleep',
            'argv': ['5', ]
        }

        self.pd_client.create_definition(definition_id, definition_type, executable, name, description)
        assert len(self.pd_client.list_definitions()) == 1

        process_id = "myprocess"

        self.pd_client.create_process(process_id, definition_id)
        self.pd_client.schedule_process(process_id, definition_id=definition_id)
        assert len(self.pd_client.describe_processes()) == 1

        # Wait for process to start running
        while True:
            procs = self.pd_client.describe_processes()
            if procs[0]['state'] >= '600':
                assert False, "Unexpected state (Never hit running?)"
            elif procs[0]['state'] < '500-RUNNING':
                time.sleep(0.1)
                continue
            elif procs[0]['state'] == '500-RUNNING':
                break

        # Wait for process to exit
        while True:
            procs = self.pd_client.describe_processes()
            if procs[0]['state'] < '800-EXITED':
                time.sleep(0.1)
                continue
            elif procs[0]['state'] == '800-EXITED':
                break
