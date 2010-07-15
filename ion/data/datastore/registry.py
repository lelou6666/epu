"""
@file ion/data/datastore/registry.py
"""

import logging
logging = logging.getLogger(__name__)

from zope import interface

from twisted.internet import defer

from ion.data import store
from ion.data import dataobject
from ion.data.datastore import objstore

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
from ion.resources import coi_resource_descriptions
import ion.util.procutils as pu

CONF = ioninit.config(__name__)


class IRegistry(object):
    """
    @brief General API of any registry
    @TOD change to use zope interface!
    """
    def clear_registry(self):
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def register_resource(self,resource):
        """
        @brief Register resource description.
        @param uuid unique name of resource instance.
        @param resource instance of OOIResource.
        @note Does the resource instance define its own name/uuid?
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def get_resource(self,resource_reference):
        """
        @param uuid name of resource.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def set_resource_lcstate(self,resource_reference,lcstate):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def set_resource_lcstate_new(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.new)

    def set_resource_lcstate_active(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.active)
        
    def set_resource_lcstate_inactive(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.inactive)

    def set_resource_lcstate_decomm(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.decomm)

    def set_resource_lcstate_retired(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.retired)

    def set_resource_lcstate_developed(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.developed)

    def set_resource_lcstate_commissioned(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.commissioned)
    
    def find_resource(self,description):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
class RegistryBackend(objstore.ObjectChassis):
    """
    """
    objectClass = dataobject.Resource

class Registry(objstore.ObjectStore, IRegistry):
    """
    """

    objectChassis = RegistryBackend

    def clear_registry(self):
        return self.backend.clear_store()



    @defer.inlineCallbacks
    def register_resource(self, resource):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        #print 'Dataobject Register Start',dataobject.DataObject._types.has_key('__builtins__')
        #del dataobject.DataObject._types['__builtins__']
        #print 'Dataobject Register Removed',dataobject.DataObject._types.has_key('__builtins__')

        if isinstance(resource, self.objectChassis.objectClass):
        
            id = resource.RegistryIdentity
            if not id:
                raise RuntimeError('Can not register a resource which does not have an identity.')

            #print 'Dataobject Register Is Instance',dataobject.DataObject._types.has_key('__builtins__')
        
            
            try:
                res_client = yield self.create(id, self.objectChassis.objectClass)
            except objstore.ObjectStoreError:
                res_client = yield self.clone(id)
 
            #print 'Dataobject Chasis',dataobject.DataObject._types.has_key('__builtins__')
            
            yield res_client.checkout()
            
            #print 'Dataobject checkout',dataobject.DataObject._types.has_key('__builtins__')
            
            res_client.index = resource
            resource.RegistryCommit = yield res_client.commit()
        else:
            resource = None
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def get_resource(self, resource_reference):
        """
        @brief Get resource description object
        """
        resource=None
        if isinstance(resource_reference, dataobject.ResourceReference):
        
            branch = resource_reference.RegistryBranch
            resource_client = yield self.clone(resource_reference.RegistryIdentity)
            if resource_client:
                if not resource_reference.RegistryCommit:
                    resource_reference.RegistryCommit = yield resource_client.get_head(branch)

                pc = resource_reference.RegistryCommit
                resource = yield resource_client.checkout(commit_id=pc)
                resource.RegistryBranch = branch
                resource.RegistryCommit = pc
            
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def set_resource_lcstate(self, resource_reference, lcstate):
        """
        Service operation: set the life cycle state of resource
        """

        resource = yield self.get_resource(resource_reference)
        
        if resource:
            resource.set_lifecyclestate(lcstate)
            resource = yield self.register_resource(resource)
           
            defer.returnValue(resource.reference())
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def _list(self):
        """
        @brief list of resource references in the registry
        @note this is a temporary solution to implement search
        """
        idlist = yield self.refs.query('([-\w]*$)')
        defer.returnValue([dataobject.ResourceReference(RegistryIdentity=id) for id in idlist])


    @defer.inlineCallbacks
    def _list_descriptions(self):
        """
        @brief list of resource descriptions in the registry
        @note this is a temporary solution to implement search
        """
        refs = yield self._list()
        defer.returnValue([(yield self.get_resource(ref)) for ref in refs])
        
        
    @defer.inlineCallbacks
    def find_resource(self,description,regex=True,ignore_defaults=True):
        """
        @brief Find resource descriptions in the registry meeting the criteria
        in the FindResourceContainer 
        """
        # container for the return arguments
        results=[]
        if isinstance(description,dataobject.Resource):
            # Get the list of descriptions in this registry
            reslist = yield self._list_descriptions()

            for res in reslist:                        
                # Test for failure and break
                test = False
                if regex:
                    if ignore_defaults:
                        test = compare_regex_no_defaults(description,res)
                    else:
                        test = compare_regex_defaults(description,res)
                else:
                    if ignore_defaults:
                        test = compare_ignore_defaults(description,res)
                    else:
                        test = compare_defaults(description,res)
                if test:
                    results.append(res)

        defer.returnValue(results)

def compare_regex_defaults(r1,r2):
    """
    test=True
    for k,v in properties.items():                
        # if this resource does not contain this attribute move on
        if not k in res.attributes:
            test = False
            break
            
        att = getattr(res, k, None)
            
        # Bogus - can't send lcstate objects in a dict must convert to sting to test
        if isinstance(att, dataobject.LCState):
            att = str(att)
            
        if isinstance(v, (str, unicode) ):
            # Use regex
            if not re.search(v, att):
                test=False
                break
        else:
            # test equality
            #@TODO add tests for range and in list...
            
            
            if att != v and v != None:
                test=False
                break                    
    """
        
def compare_regex_no_defaults(r1,r2):
    """
    """
        
def compare_defaults(r1,r2):
    try:
        m = [getattr(r1, a) == getattr(r2, a) for a in r1.attributes]
        return reduce(lambda a, b: a and b, m)
    except:
        return False
        
def compare_ignore_defaults(r1,r2):
    """
    """
    try:
        default = r1.__class__()
        m = [getattr(r1, a) == getattr(r2, a) or getattr(r1, a) ==  getattr(default,a) for a in r1.attributes]
        return reduce(lambda a, b: a and b, m)
    except:
        return False
            


@defer.inlineCallbacks
def test(ns):
    from ion.data import store
    s = yield store.Store.create_store()
    ns.update(locals())
    reg = yield ResourceRegistry.new(s, 'registry')
    res1 = dataobject.Resource.create_new_resource()
    ns.update(locals())
    res1.name = 'foo'
    commit_id = yield reg.register_resource(res1)
    res2 = dataobject.Resource.create_new_resource()
    res2.name = 'doo'
    commit_id = yield reg.register_resource(res2)
    ns.update(locals())



class BaseRegistryService(BaseService):
    """
    @Brief Base Registry Service Clase
    """

    
    # For now, keep registration in local memory store. override with spawn args to use cassandra
    @defer.inlineCallbacks
    def slc_init(self):
        # use spawn args to determine backend class, second config file
        backendcls = self.spawn_args.get('backend_class', CONF.getValue('backend_class', None))
        backendargs = self.spawn_args.get('backend_args', CONF.getValue('backend_args', {}))
        if backendcls:
            self.backend = pu.get_class(backendcls)
        else:
            self.backend = store.Store
        assert issubclass(self.backend, store.IStore)

        # Provide rest of the spawnArgs to init the store
        s = yield self.backend.create_store(**backendargs)
        
        self.reg = Registry(s)
        
        name = self.__class__.__name__
        logging.info(name + " initialized")
        logging.info(name + " backend:"+str(backendcls))
        logging.info(name + " backend args:"+str(backendargs))


    @defer.inlineCallbacks
    def base_clear_registry(self, content, headers, msg):
        logging.info('op_clear_registry!')
        yield self.reg.clear_registry()
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def base_register_resource(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        resource = dataobject.Resource.decode(content)()
        logging.info('op_register_resource: \n' + str(resource))
  
        resource = yield self.reg.register_resource(resource)
        if resource:
            yield self.reply_ok(msg, resource.encode())
        else:
            yield self.reply_err(msg, None)


    @defer.inlineCallbacks
    def base_get_resource(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        resource_reference = dataobject.Resource.decode(content)()
        logging.info('op_get_resource: '+str(resource_reference))

        resource = yield self.reg.get_resource(resource_reference)
        logging.info('Got Resource:\n'+str(resource))
        if resource:
            yield self.reply_ok(msg, resource.encode())
        else:
            yield self.reply_err(msg, None)

        
    @defer.inlineCallbacks
    def base_set_resource_lcstate(self, content, headers, msg):
        """
        Service operation: set the life cycle state of resource
        """
        container = dataobject.Resource.decode(content)()

        if isinstance(reference_lcstate,  coi_resources.SetResourceLCStateContainer):
            logging.info('op_set_resource_lcstate: '+str(container))
            resource_reference = container.reference
            lcstate = container.lcstate

            resource = yield self.reg.set_resource_lcstate(resource_reference, lcstate)
        
            if resource:
                yield self.reply_ok(msg, resource.reference().encode())

        else:
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def base_find_resource(self, content, headers, msg):
        """
        @brief Find resource descriptions in the registry meeting the criteria
        listed in the properties dictionary 
        """
        description = None
        regex = None
        ignore_defaults = None
                
        container = dataobject.Resource.decode(content)()
        
        result_list = []
        if isinstance(container,  coi_resource_descriptions.FindResourceContainer):
            description = container.description
            regex = container.regex
            ignore_defaults = container.ignore_defaults
            
            result_list = yield self.reg.find_resource(description,regex,ignore_defaults)
        
        results=coi_resource_descriptions.ResourceListContainer()
        results.resources = result_list
        
        yield self.reply_ok(msg, results.encode())


class RegistryService(BaseRegistryService):

     # Declaration of service
    declare = BaseService.service_declare(name='registry_service', version='0.1.0', dependencies=[])

    op_clear_registry = BaseRegistryService.base_clear_registry
    op_register_resource = BaseRegistryService.base_register_resource
    op_get_resource = BaseRegistryService.base_get_resource
    op_set_resource_lcstate = BaseRegistryService.base_set_resource_lcstate
    op_find_resource = BaseRegistryService.base_find_resource


# Spawn of the process using the module name
factory = ProtocolFactory(RegistryService)






class BaseRegistryClient(BaseServiceClient,IRegistry):
    """
    Do not instantiate this class!
    """
            
    @defer.inlineCallbacks
    def base_clear_registry(self,op_name):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send(op_name,None)
        if content['status']=='OK':
            defer.returnValue(None)


    @defer.inlineCallbacks
    def base_register_resource(self,resource,op_name):
        """
        @brief Store a resource in the registry by its ID. It can be new or
        modified.
        @param res_id is a resource identifier unique to this resource.
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send(op_name,
                                            resource.encode())
        logging.info('Service reply: '+str(headers))
        if content['status']=='OK':
            resource = dataobject.Resource.decode(content['value'])()
            defer.returnValue(resource)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def base_get_resource(self,resource_reference,op_name):
        """
        @brief Retrieve a resource from the registry by Reference
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send(op_name,
                                                      resource_reference.encode())
        logging.info('Service reply: '+str(headers))

        if content['status']=='OK':
            resource = dataobject.Resource.decode(content['value'])()
            defer.returnValue(resource)
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def set_resource_lcstate(self, resource_reference, lcstate):
        """
        @brief Retrieve a resource from the registry by its ID
        """
        yield self._check_init()

        container = coi_resources.SetResourceLCStateContainer()
        container.lcstate = lcstate
        container.reference = resource_reference

        (content, headers, msg) = yield self.rpc_send('set_resource_lcstate',
                                                      container.encode())
        logging.info('Service reply: '+str(headers))
        
        if content['status'] == 'OK':
            resource_reference = dataobject.ResourceReference.decode(content['value'])()
            defer.returnValue(resource_reference)
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def find_resource(self,description,regex=True,ignore_defaults=True):
        """
        @brief Retrieve all the resources in the registry
        @param attributes is a dictionary of attributes which will be used to select a resource
        """
        yield self._check_init()

        container = coi_resource_descriptions.FindResourceContainer()
        container.description = description
        container.ignore_defaults = ignore_defaults
        container.regex = regex
        
        (content, headers, msg) = yield self.rpc_send('find_resource',
                                                      container.encode())
        logging.info('Service reply: '+str(headers))
        
        # Return a list of resources
        if content['status'] == 'OK':            
            results = dataobject.DataObject.decode(content['value'])()
            defer.returnValue(results.resources)
        else:
            defer.returnValue([])


class RegistryClient(BaseRegistryClient,IRegistry):
    """
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "registry_service"
        BaseServiceClient.__init__(self, proc, **kwargs)


    def clear_registry(self):
        return self.base_clear_registry(self,'clear_registry')


    def register_resource(self,resource):
        return self.base_register_resource(self,resource, 'register_resource')

    def get_resource(self,resource_reference):
        return self.base_get_resource(self,resource_reference,'get_resource'):






