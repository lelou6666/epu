<<<<<<< HEAD

import logging
import uuid

from socket import timeout

log = logging.getLogger(__name__)  
=======
# Copyright 2013 University of Chicago


import logging

from epu.exceptions import ProgrammingError, PolicyError
from epu.highavailability.policy import policy_map

log = logging.getLogger(__name__)


class IProcessControl(object):

    def schedule_process(self, pd_name, process_definition_id, **kwargs):
        """Launches a new process on the specified process dispatcher

        Returns upid of process
        """

    def terminate_process(self, upid):
        """Terminates a process in the system
        """

    def get_all_processes(self):
        """Gets a dictionary of lists of {"upid": "XXXX", "state": "XXXXX"} dicts
        """

>>>>>>> refs/remotes/nimbusproject/master

class HighAvailabilityCore(object):
    """Core of High Availability Service
    """

<<<<<<< HEAD
    def __init__(self, CFG, pd_client_kls, process_dispatchers, process_spec, Policy):
        """Create HighAvailabilityCore

        @param CFG - config dictionary for highavailabilty
        @param pd_client_kls - a constructor method for creating a 
               ProcessDispatcherClient that takes one argument, the topic
=======
    def __init__(self, CFG, control, process_dispatchers, policy,
            process_definition_id=None, process_configuration=None,
            parameters=None, aggregator_config=None, name=None):
        """Create HighAvailabilityCore

        @param CFG - config dictionary for highavailabilty
        @param control - process control object. interface of IPolicyControl
>>>>>>> refs/remotes/nimbusproject/master
        @param process_dispatchers - list of process dispatchers
        """

        self.CFG = CFG
<<<<<<< HEAD
        self.provisioner_client_kls = pd_client_kls
        self.process_dispatchers = process_dispatchers
        self.process_spec = process_spec
        self.policy_params = None
        self.policy = Policy(parameters=self.policy_params,
                dispatch_process_callback=self._dispatch_pd_spec,
                terminate_process_callback=self._terminate_upid, 
                process_spec=self.process_spec)
        self.managed_upids = []

    def apply_policy(self):
        """Should be run periodically by dashi/pyon proc container to check 
        status of services, and balance to compensate for changes
        """
        log.debug("applying policy")

        all_procs = self._query_process_dispatchers()
        self.managed_upids = list(self.policy.apply_policy(all_procs, self.managed_upids))


    def _query_process_dispatchers(self):
        """Get list of processes from each pd, and return a dictionary
        indexed by the pd name
        """
        all_procs = {}

        for pd_name in self.process_dispatchers:
            pd_client = self._get_pd_client(pd_name)
            try:
                procs = pd_client.describe_processes()
                all_procs[pd_name] = procs
            except timeout:
                log.warning("%s timed out when calling describe_processes" % pd_name)
            except:
                log.exception("Problem querying %s" % pd_name)

        return all_procs

    def _get_pd_client(self, name):
        """Returns a process dispatcher client with the topic/name
        provided, using the process dispatcher client class provided
        in the constructor
        """
        return self.provisioner_client_kls(name)

    def _dispatch_pd_spec(self, pd_name, spec):
        """Dispatches a process to the provided pd, and returns the upid used
        to do so
        """
        pd_client = self._get_pd_client(pd_name)
        upid = uuid.uuid4().hex
        
        pd_client.dispatch_process(upid, spec, None, None)
        self.managed_upids.append(upid)

=======
        self.control = control
        self.policy_type = None
        self.process_dispatchers = process_dispatchers
        self.process_configuration = process_configuration
        self.aggregator_config = aggregator_config
        self.name = name
        if self.name:
            self.logprefix = "HA Agent (%s): " % self.name
        else:
            self.logprefix = ""

        if not process_definition_id:
            raise ProgrammingError("You must have a process_definition_id")
        self.process_definition_id = process_definition_id

        self.reconfigure_policy(parameters, policy)
        self.managed_upids = []

    def apply_policy(self):
        """Should be run periodically by dashi/pyon proc container to check
        status of services, and balance to compensate for changes
        """
        log.debug("%sapplying policy", self.logprefix)

        all_procs = self.control.get_all_processes()
        try:
            managed_upids = self.policy.apply_policy(all_procs, self.managed_upids)
            if isinstance(managed_upids, (tuple, list)):
                self.managed_upids = managed_upids
        except PolicyError:
            log.exception("Couldn't apply policy because of an error")

    def set_managed_upids(self, upids):
        """Called to override the managed process set, for HAAgent restart
        """
        self.managed_upids = list(upids)

    def _schedule(self, pd_name, pd_id, configuration=None, constraints=None,
                  queueing_mode=None, restart_mode=None,
                  execution_engine_id=None, node_exclusive=None):
        """Dispatches a process to the provided pd, and returns the upid used
        to do so
        """
        try:

            upid = self.control.schedule_process(pd_name, pd_id,
                configuration=configuration, constraints=constraints,
                queueing_mode=queueing_mode, restart_mode=restart_mode,
                execution_engine_id=execution_engine_id,
                node_exclusive=node_exclusive)

        except Exception:
            log.exception("%sProblem scheduling proc on '%s'. Will try again later", self.logprefix, pd_name)
            return None
        self.managed_upids.append(upid)
>>>>>>> refs/remotes/nimbusproject/master
        return upid

    def _terminate_upid(self, upid):
        """Finds a upid among available PDs, and terminates it
        """
<<<<<<< HEAD
        all_procs = self._query_process_dispatchers()
        for pd_name, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    pd_client = self._get_pd_client(pd_name)
                    pd_client.terminate_process(upid)
                    self.managed_upids.remove(upid)
                    return upid

        return None


    def reconfigure_policy(self, new_policy):
        """Change the number of needed instances of service
        """
        self.policy_params = new_policy
        self.policy.parameters = new_policy
=======
        try:
            self.control.terminate_process(upid)
            self.managed_upids.remove(upid)
            return upid
        except Exception:
            log.exception("%sProblem terminating process '%s'. Will try again later", self.logprefix, upid)

        return None

    def _process_state(self, upid):
        """Finds a upid among available PDs, and gets its status
        """
        all_procs = self.control.get_all_processes()
        for pd_name, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    return proc.get('state')

        return None

    def status(self):
        """Returns a single status for the current state of the service
        """
        return self.policy.status()

    def reconfigure_policy(self, new_policy_params, new_policy=None):
        """Reconfigure the policy of this ha service
        """
        if new_policy is not None and new_policy != self.policy_type:
            Policy = policy_map.get(new_policy)
            if Policy is None:
                raise PolicyError("HA doesn't know how to use %s policy" % new_policy)
            self.policy = Policy(parameters=new_policy_params,
                    schedule_process_callback=self._schedule,
                    terminate_process_callback=self._terminate_upid,
                    process_state_callback=self._process_state,
                    process_definition_id=self.process_definition_id,
                    process_configuration=self.process_configuration,
                    aggregator_config=self.aggregator_config, name=self.name)
            self.policy_type = new_policy
        elif new_policy_params is not None:
            self.policy.parameters = new_policy_params
>>>>>>> refs/remotes/nimbusproject/master

    def dump(self):

        state = {}
<<<<<<< HEAD
        state['policy'] = self.policy_params
=======
        state['policy'] = self.policy_type
        state['policy_params'] = self.policy.parameters
>>>>>>> refs/remotes/nimbusproject/master
        state['managed_upids'] = self.managed_upids

        return state
