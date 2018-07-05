import ctypes
import os
import sys
import weakref



CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

#4 is machine
cpu_arch = os.uname()[4]

rti_arch = {
    'x86_64': 'x64Linux',
    'x86': 'i86Linux',
    'aarch64': '<TODO add here arm prefix>'
}[cpu_arch]

connector_so = os.path.join(CURRENT_DIR, {
    'x86_64': 'librtiddsconnector-x64.so',
    'aarch64': 'librtiddsconnector-armv8.so'
}[cpu_arch])

_ddsc_lib = None

if os.path.isfile(connector_so):
    _ddsc_lib = ctypes.CDLL(connector_so) 

if not _ddsc_lib:
    NDDSHOME = os.environ['NDDSHOME']
    archs = os.listdir(os.path.join(NDDSHOME, 'lib'))

    arch_str = None
    for arch in archs:
        if arch.startswith(rti_arch):
            arch_str = arch
            break

    if arch_str is None:
        raise Exception("No sutiable RTI installation was found for '%s' arch in %s" % (cpu_arch, NDDSHOME))

    base_path = os.path.join(NDDSHOME, 'lib', arch_str)
    _ddscore_lib = ctypes.CDLL(os.path.join(base_path, 'libnddscore.so'), ctypes.RTLD_GLOBAL)
    _ddsc_lib = ctypes.CDLL(os.path.join(base_path, 'libnddsc.so'))


# Python 3 has bytes and str, calling ctypes requires bytes and returns bytes
# the following functions deal with this conversion
if sys.version_info >= (3, 0):
    def cstring(s):
        if isinstance(s, str):
            return bytes(s, 'ascii')
        elif isinstance(s ,bytes):
            return s
        else:
            raise ArgumentException("%s is not a string" % s)

    def pstring(b):
        return str(b, 'ascii')
else:
    pstring = cstring = lambda x: x

# Error checkers

class Error(Exception):
    pass

def check_code(result, func, arguments):
    if result != 0:
        raise Error({
            1: 'error',
            2: 'unsupported',
            3: 'bad parameter',
            4: 'precondition not met',
            5: 'out of resources',
            6: 'not enabled',
            7: 'immutable policy',
            8: 'inconsistant policy',
            9: 'already deleted',
            10: 'timeout',
            11: 'no data',
            12: 'illegal operation',
        }[result])

def check_null(result, func, arguments):
    if not result:
        raise Error()
    return result

def check_ex(result, func, arguments):
    if arguments[-1]._obj.value != 0:
        raise Error({
            1: '(user)',
            2: '(system)',
            3: 'bad param (system)',
            4: 'no memory (system)',
            5: 'bad typecode (system)',
            6: 'badkind (user)',
            7: 'bounds (user)',
            8: 'immutable typecode (system)',
            9: 'bad member name (user)',
            10: 'bad member id (user)',
        }[arguments[-1]._obj.value])
    return result

def check_true(result, func, arguments):
    if not result:
        raise Error()

# Function and structure accessors

def get(name, type):
    return ctypes.cast(getattr(_ddsc_lib, 'DDS_' + name), ctypes.POINTER(type)).contents



class DDSFunc(object):
    pass

DDSFunc = DDSFunc()

class DDSType(object):
    def __getattr__(self, attr):
        contents = type(attr, (ctypes.Structure,), {})
        
        def g(self2, attr2):
            f = getattr(DDSFunc, attr + '_' + attr2)
            def m(*args):
                return f(self2, *args)
            setattr(self2, attr2, m)
            return m
        # make structs dynamically present bound methods
        contents.__getattr__ = g
        # take advantage of POINTERs being cached to make type pointers do the same
        ctypes.POINTER(contents).__getattr__ = g
        
        setattr(self, attr, contents)
        return contents

DDSType = DDSType()

DDSType.Topic._fields_ = [
    ('_as_Entity', ctypes.c_void_p),
    ('_as_TopicDescription', ctypes.POINTER(DDSType.TopicDescription)),
]
ctypes.POINTER(DDSType.Topic).as_topicdescription = lambda self: self.contents._as_TopicDescription

DDSType.DynamicDataSeq._fields_ = DDSType.SampleInfoSeq._fields_ = [
    ('_owned', ctypes.c_bool),
    ('_contiguous_buffer', ctypes.c_void_p),
    ('_discontiguous_buffer', ctypes.c_void_p),
    ('_maximum', ctypes.c_ulong),
    ('_length', ctypes.c_ulong),
    ('_sequence_init', ctypes.c_long),
    ('_read_token1', ctypes.c_void_p),
    ('_read_token2', ctypes.c_void_p),
    ('_elementPointersAllocation', ctypes.c_bool),
]

DDSType.InstanceHandle_t._fields_ = [
    ('keyHash_value', ctypes.c_byte * 16),
    ('keyHash_length', ctypes.c_uint32),
    ('isValid', ctypes.c_int),
]
DDS_HANDLE_NIL = DDSType.InstanceHandle_t((ctypes.c_byte * 16)(*[0]*16), 16, False)
DDS_LENGTH_UNLIMITED = 2^16

DDS_Long = ctypes.c_long


DDSType.DDS_Time_t._fields_ = [
    ('sec', ctypes.c_long),
    ('nanosec', ctypes.c_ulong),
]

DDS_SequenceNumber_t = ctypes.c_int
DDS_SampleStateKind = ctypes.c_int
DDS_ViewStateKind = ctypes.c_int
DDS_InstanceStateKind = ctypes.c_int
DDS_Boolean = ctypes.c_bool
DDS_GUID_t = ctypes.c_long

DDSType.SampleInfo._fields_ = [
    ('sample_state', DDS_SampleStateKind),
    ('view_state', DDS_ViewStateKind),
    ('instance_state', DDS_InstanceStateKind),
    ('source_timestamp', DDSType.DDS_Time_t),
    ('instance_handle', DDSType.InstanceHandle_t),
    ('publication_handle', DDSType.InstanceHandle_t),
    ('disposed_generation_count', DDS_Long),
    ('no_writers_generation_count', DDS_Long),
    ('sample_rank', DDS_Long),
    ('generation_rank', DDS_Long),
    ('absolute_generation_rank', DDS_Long),
    ('valid_data', DDS_Boolean),
    ('reception_timestamp', DDSType.DDS_Time_t),
    ('publication_sequence_number', DDS_SequenceNumber_t),
    ('reception_sequence_number', DDS_SequenceNumber_t),
    ('publication_virtual_guid', DDS_GUID_t),
    ('publication_virtual_sequence_number', DDS_SequenceNumber_t),
    ('original_publication_virtual_guid', DDS_GUID_t),
    ('original_publication_virtual_sequence_number', DDS_SequenceNumber_t),
]


# class SampleInfo(ctypes.Structure):
#     _fields_=[('instance_state', ctypes.c_uint32)]

#ctypes.POINTER(DDSType.SampleInfo).instance_state = lambda self: self.contents.instance_state


# some types
enum = ctypes.c_int

DDS_Char = ctypes.c_char
DDS_Wchar = ctypes.c_wchar
DDS_Octet = ctypes.c_ubyte
DDS_Short = ctypes.c_int16
DDS_UnsignedShort = ctypes.c_uint16
DDS_Long = ctypes.c_int32
DDS_UnsignedLong = ctypes.c_uint32
DDS_LongLong = ctypes.c_int64
DDS_UnsignedLongLong = ctypes.c_uint64
DDS_Float = ctypes.c_float
DDS_Double = ctypes.c_double
DDS_LongDouble = ctypes.c_longdouble
DDS_Boolean = ctypes.c_bool
DDS_Enum = DDS_UnsignedLong

DDS_DynamicDataMemberId = DDS_Long
DDS_ReturnCode_t = enum
DDS_ExceptionCode_t = enum
def ex():
    return ctypes.byref(DDS_ExceptionCode_t())
DDS_DomainId_t = ctypes.c_int32
DDS_TCKind = enum

DDS_SampleStateMask = DDS_UnsignedLong
DDS_ViewStateMask = DDS_UnsignedLong
DDS_InstanceStateMask = DDS_UnsignedLong
DDS_StatusMask = DDS_UnsignedLong

DDS_InstanceStateKind = ctypes.c_uint32

DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED = 0

DDSType.Listener._fields_ = [
    ('listener_data', ctypes.c_void_p),
]

DDSType.DataReaderListener._fields_ = [
    ('as_listener', DDSType.Listener),
    ('on_requested_deadline_missed', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.RequestedDeadlineMissedStatus))),
    ('on_requested_incompatible_qos', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.RequestedIncompatibleQosStatus))),
    ('on_sample_rejected', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.SampleRejectedStatus))),
    ('on_liveliness_changed', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.LivelinessChangedStatus))),
    ('on_data_available', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader))),
    ('on_subscription_matched', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.SubscriptionMatchedStatus))),
    ('on_sample_lost', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.SampleLostStatus))),
]

class TCKind(object):
    NULL = 0
    SHORT = 1
    LONG = 2
    USHORT = 3
    ULONG = 4
    FLOAT = 5
    DOUBLE = 6
    BOOLEAN = 7
    CHAR = 8
    OCTET = 9
    STRUCT = 10
    UNION = 11
    ENUM = 12
    STRING = 13
    SEQUENCE = 14
    ARRAY = 15
    ALIAS = 16
    LONGLONG = 17
    ULONGLONG = 18
    LONGDOUBLE = 19
    WCHAR = 20
    WSTRING = 21
    VALUE = 22
    SPARSE = 23
    RAW_BYTES = 0x7e
    RAW_BYTES_KEYED = 0x7f

DATA_AVAILABLE_STATUS = 0x0001 << 10

# Function prototypes

_dyn_basic_types = {
    TCKind.LONG: ('long', DDS_Long, (-2**31, 2**31)),
    TCKind.ULONG: ('ulong', DDS_UnsignedLong, (0, 2**32)),
    TCKind.SHORT: ('short', DDS_Short, (-2**15, 2**15)),
    TCKind.USHORT: ('ushort', DDS_UnsignedShort, (0, 2**16)),
    TCKind.LONGLONG: ('longlong', DDS_LongLong, (-2**63, 2**63)),
    TCKind.ULONGLONG: ('ulonglong', DDS_UnsignedLongLong, (0, 2**64)),
    TCKind.FLOAT: ('float', DDS_Float, None),
    TCKind.DOUBLE: ('double', DDS_Double, None),
    TCKind.BOOLEAN: ('boolean', DDS_Boolean, None),
    TCKind.OCTET: ('octet', DDS_Octet, (0, 2**8)),
    TCKind.CHAR: ('char', DDS_Char, None),
    TCKind.WCHAR: ('wchar', DDS_Wchar, None),
}

def _define_func(params):
    p, errcheck, restype, argtypes = params
    f = getattr(_ddsc_lib, 'DDS_' + p)
    if errcheck is not None:
        f.errcheck = errcheck
    f.restype = restype
    f.argtypes = argtypes
    setattr(DDSFunc, p, f)

list(map(_define_func, [
    ('DomainParticipantFactory_get_instance', check_null, ctypes.POINTER(DDSType.DomainParticipantFactory), []),
    ('DomainParticipantFactory_create_participant', check_null, ctypes.POINTER(DDSType.DomainParticipant), [ctypes.POINTER(DDSType.DomainParticipantFactory), DDS_DomainId_t, ctypes.POINTER(DDSType.DomainParticipantQos), ctypes.POINTER(DDSType.DomainParticipantListener), DDS_StatusMask]),
    ('DomainParticipantFactory_create_participant_from_config', check_null, ctypes.POINTER(DDSType.DomainParticipant), [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.c_char_p]),
    ('DomainParticipantFactory_delete_participant', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.POINTER(DDSType.DomainParticipant)]),
    
    ('DomainParticipant_create_publisher', check_null, ctypes.POINTER(DDSType.Publisher), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.PublisherQos), ctypes.POINTER(DDSType.PublisherListener), DDS_StatusMask]),
    ('DomainParticipant_delete_publisher', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Publisher)]),
    ('DomainParticipant_create_subscriber', check_null, ctypes.POINTER(DDSType.Subscriber), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.SubscriberQos), ctypes.POINTER(DDSType.SubscriberListener), DDS_StatusMask]),
    ('DomainParticipant_delete_subscriber', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Subscriber)]),
    ('DomainParticipant_create_topic', check_null, ctypes.POINTER(DDSType.Topic), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(DDSType.TopicQos), ctypes.POINTER(DDSType.TopicListener), DDS_StatusMask]),
    ('DomainParticipant_delete_topic', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Topic)]),
    ('DomainParticipant_lookup_datawriter_by_name', check_null, ctypes.POINTER(DDSType.DataWriter), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    ('DomainParticipant_lookup_datareader_by_name', check_null, ctypes.POINTER(DDSType.DataReader), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    ('DomainParticipant_delete_contained_entities', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant)]),

    ('Publisher_create_datawriter', check_null, ctypes.POINTER(DDSType.DataWriter), [ctypes.POINTER(DDSType.Publisher), ctypes.POINTER(DDSType.Topic), ctypes.POINTER(DDSType.DataWriterQos), ctypes.POINTER(DDSType.DataWriterListener), DDS_StatusMask]),
    ('Publisher_delete_datawriter', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.Publisher), ctypes.POINTER(DDSType.DataWriter)]),
    
    ('Subscriber_create_datareader', check_null, ctypes.POINTER(DDSType.DataReader), [ctypes.POINTER(DDSType.Subscriber), ctypes.POINTER(DDSType.TopicDescription), ctypes.POINTER(DDSType.DataReaderQos), ctypes.POINTER(DDSType.DataReaderListener), DDS_StatusMask]),
    ('Subscriber_delete_datareader', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.Subscriber), ctypes.POINTER(DDSType.DataReader)]),
    
    ('DataWriter_get_topic', check_null, ctypes.POINTER(DDSType.Topic), [ctypes.POINTER(DDSType.DataWriter)]),
    ('DynamicDataWriter_create_data_w_property', check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.DynamicDataWriter),ctypes.POINTER(DDSType.DynamicDataProperty_t) ]),
    ('DynamicDataWriter_delete_data', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData)]),


    ('DataReader_set_listener', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.DataReaderListener), DDS_StatusMask]),
    
    ('TopicDescription_get_type_name',check_null, ctypes.c_char_p, [ctypes.POINTER(DDSType.Topic)]),

    ('DynamicDataTypeSupport_new', check_null, ctypes.POINTER(DDSType.DynamicDataTypeSupport), [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDSType.DynamicDataTypeProperty_t)]),
    ('DynamicDataTypeSupport_delete', None, None, [ctypes.POINTER(DDSType.DynamicDataTypeSupport)]),
    ('DynamicDataTypeSupport_register_type', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    ('DynamicDataTypeSupport_unregister_type', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    ('DynamicDataTypeSupport_create_data', check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.DynamicDataTypeSupport)]),
    ('DynamicDataTypeSupport_delete_data', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicDataTypeSupport_print_data', None, None, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DynamicData)]),
    
    ('DynamicData_new', check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDSType.DynamicDataProperty_t)]),
] + [
    ('DynamicData_get_' + func_name, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(data_type), ctypes.c_char_p, DDS_DynamicDataMemberId])
        for func_name, data_type, bounds in _dyn_basic_types.values()
] + [
    ('DynamicData_set_' + func_name, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, data_type])
        for func_name, data_type, bounds  in _dyn_basic_types.values()
] + [
    ('DynamicData_get_string', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(DDS_UnsignedLong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_get_wstring', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.c_wchar_p), ctypes.POINTER(DDS_UnsignedLong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_set_string', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, ctypes.c_char_p]),
    ('DynamicData_set_wstring', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, ctypes.c_wchar_p]),
    ('DynamicData_bind_complex_member', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_unbind_complex_member', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_get_member_type', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.POINTER(DDSType.TypeCode)), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_get_member_count', None, DDS_UnsignedLong, [ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_get_type', check_null, ctypes.POINTER(DDSType.TypeCode), [ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_get_type_kind', None, DDS_TCKind, [ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_delete', None, None, [ctypes.POINTER(DDSType.DynamicData)]),
    
    ('DynamicDataWriter_narrow', check_null, ctypes.POINTER(DDSType.DynamicDataWriter), [ctypes.POINTER(DDSType.DataWriter)]),
    ('DynamicDataWriter_write', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataWriter), ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),
    ('DynamicDataWriter_dispose', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataWriter), ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),
    ('DynamicDataWriter_unregister_instance', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataWriter), ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),

    ('DynamicDataReader_narrow', check_null, ctypes.POINTER(DDSType.DynamicDataReader), [ctypes.POINTER(DDSType.DataReader)]),
    ('DynamicDataReader_take', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long, DDS_SampleStateMask, DDS_ViewStateMask, DDS_InstanceStateMask]),
    ('DynamicDataReader_read', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long, DDS_SampleStateMask, DDS_ViewStateMask, DDS_InstanceStateMask]),
    ('DynamicDataReader_return_loan', check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq)]),
    
    ('TypeCode_name', check_ex, ctypes.c_char_p, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_kind', check_ex, DDS_TCKind, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_member_count', check_ex, DDS_UnsignedLong, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_member_name', check_ex, ctypes.c_char_p, [ctypes.POINTER(DDSType.TypeCode), DDS_UnsignedLong, ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_member_type', check_ex, ctypes.POINTER(DDSType.TypeCode), [ctypes.POINTER(DDSType.TypeCode), DDS_UnsignedLong, ctypes.POINTER(DDS_ExceptionCode_t)]),
    
    ('DynamicDataSeq_initialize', check_true, DDS_Boolean, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    ('DynamicDataSeq_get_length', None, DDS_Long, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    ('DynamicDataSeq_get_reference', check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.DynamicDataSeq), DDS_Long]),
    
    ('SampleInfoSeq_initialize', check_true, DDS_Boolean, [ctypes.POINTER(DDSType.SampleInfoSeq)]),
    ('SampleInfoSeq_get_length', None, DDS_Long, [ctypes.POINTER(DDSType.SampleInfoSeq)]),
    ('SampleInfoSeq_get_reference', check_null, ctypes.POINTER(DDSType.SampleInfo), [ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long]),
    
    ('String_free', None, None, [ctypes.c_char_p]),
    
    ('Wstring_free', None, None, [ctypes.c_wchar_p]),
]))

def write_into_dd_member(obj, dd, member_name=None, member_id=DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED):
    member_name = cstring(member_name)

    tc = ctypes.POINTER(DDSType.TypeCode)()
    dd.get_member_type(ctypes.byref(tc), member_name, member_id, ex())
    
    kind = tc.kind(ex())
    if kind in _dyn_basic_types:
        func_name, data_type, bounds = _dyn_basic_types[kind]
        if bounds is not None and not bounds[0] <= obj < bounds[1]:
            raise ValueError('%r not in range [%r, %r)' % (obj, bounds[0], bounds[1]))
        getattr(dd, 'set_' + func_name)(member_name, member_id, obj)
    elif kind == TCKind.STRUCT or kind == TCKind.SEQUENCE or kind == TCKind.ARRAY:
        inner = DDSFunc.DynamicData_new(None, get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty_t))
        try:
            dd.bind_complex_member(inner, member_name, member_id)
            try:
                write_into_dd(obj, inner)
            finally:
                dd.unbind_complex_member(inner)
        finally:
            inner.delete()
    elif kind == TCKind.STRING:
        if '\0' in obj:
            raise ValueError('strings can not contain null characters')
        dd.set_string(member_name, member_id, cstring(obj))
    elif kind == TCKind.WSTRING:
        dd.set_wstring(member_name, member_id, obj)
    elif kind == TCKind.ENUM:
        dd.set_long(member_name, member_id, obj)
    else:
        raise NotImplementedError(kind)

def write_into_dd(obj, dd):
    kind = dd.get_type_kind()
    if kind == TCKind.STRUCT:
        assert isinstance(obj, dict)
        tc = dd.get_type()
        for i in range(tc.member_count(ex())):
            name = pstring(tc.member_name(i, ex()))
            if name in obj:
                write_into_dd_member(obj[name], dd, member_name=name)
    elif kind == TCKind.ARRAY or kind == TCKind.SEQUENCE:
        assert isinstance(obj, list)
        for i, x in enumerate(obj):
            write_into_dd_member(x, dd, member_id=i+1)
    else:
        raise NotImplementedError(kind)

def unpack_dd_member(dd, member_name=None, member_id=DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED):
    tc = ctypes.POINTER(DDSType.TypeCode)()
    dd.get_member_type(ctypes.byref(tc), member_name, member_id, ex())
    
    kind = tc.kind(ex())
    if kind in _dyn_basic_types:
        func_name, data_type, bounds = _dyn_basic_types[kind]
        inner = data_type()
        getattr(dd, 'get_' + func_name)(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.STRUCT or kind == TCKind.SEQUENCE or kind == TCKind.ARRAY:
        inner = DDSFunc.DynamicData_new(None, get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty_t))
        try:
            dd.bind_complex_member(inner, member_name, member_id)
            try:
                return unpack_dd(inner)
            finally:
                dd.unbind_complex_member(inner)
        finally:
            inner.delete()
    elif kind == TCKind.STRING:
        inner = ctypes.c_char_p(None)
        try:
            dd.get_string(ctypes.byref(inner), None, member_name, member_id)
            return pstring(inner.value)
        finally:
            DDSFunc.String_free(inner)
    elif kind == TCKind.WSTRING:
        inner = ctypes.c_wchar_p(None)
        try:
            dd.get_wstring(ctypes.byref(inner), None, member_name, member_id)
            return inner.value
        finally:
            DDSFunc.Wstring_free(inner)
    elif kind == TCKind.ENUM:
        inner = DDS_Long()
        dd.get_long(ctypes.byref(inner), member_name, member_id)
        return inner.value
    else:
        raise NotImplementedError(kind)

def unpack_dd(dd):
    kind = dd.get_type_kind()
    if kind == TCKind.STRUCT:
        obj = {}
        tc = dd.get_type()
        for i in range(tc.member_count(ex())):
            name = tc.member_name(i, ex())
            obj[pstring(name)] = unpack_dd_member(dd, member_name=name)
        return obj
    elif kind == TCKind.ARRAY or kind == TCKind.SEQUENCE:
        obj = []
        for i in range(dd.get_member_count()):
            obj.append(unpack_dd_member(dd, member_id=i+1))
        return obj
    else:
        raise NotImplementedError(kind)

_outside_refs = set()
_refs = set()


def unpack_sampleInfo(sampleInfo):
    obj = {}
    obj['InstanceState']=sampleInfo.contents.instance_state
    obj['SampleState']=sampleInfo.contents.sample_state
    obj['ViewState']=sampleInfo.contents.view_state
    return obj

class Writer(object):
    def __init__(self, dds, name):
        self._dds = weakref.ref(dds)
        self.name = name
        self._writer = dds._participant.lookup_datawriter_by_name(cstring(name))
        self._dyn_narrowed_writer = DDSFunc.DynamicDataWriter_narrow(self._writer)
        self._dynamicData = self._dyn_narrowed_writer.create_data_w_property(get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty_t))

    def __del__(self):
        # TODO: what about this?
        #self._dyn_narrowed_writer.delete_data(self._dynamicData)
        pass
    
    def write(self, msg):
        write_into_dd(msg, self._dynamicData)
        self._dyn_narrowed_writer.write(self._dynamicData, DDS_HANDLE_NIL)

    def dispose(self, msg):
        write_into_dd(msg, self._dynamicData)
        self._dyn_narrowed_writer.dispose(self._dynamicData, DDS_HANDLE_NIL)

    def unregister(self, msg):
        write_into_dd(msg, self._dynamicData)
        self._dyn_narrowed_writer.unregister_instance(self._dynamicData, DDS_HANDLE_NIL)

class Reader(object):
    def __init__(self, dds, name):
    
        self._dds = weakref.ref(dds)
        self.name = name
        self._reader = dds._participant.lookup_datareader_by_name(cstring(name))
        self._dyn_narrowed_reader = DDSFunc.DynamicDataReader_narrow(self._reader)
        self._callbacks = {}
    
    def __del__(self):
        pass

    def _enable_listener(self):
        assert self._listener is None
        self._listener = DDSType.DataReaderListener(on_data_available=ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader))(self._data_available_callback))
        self._reader.set_listener(self._listener, DATA_AVAILABLE_STATUS)
        _outside_refs.add(self) # really want self._listener, but this does the same thing
    
    def _disable_listener(self):
        assert self._listener is not None
        self._reader.set_listener(None, 0)
        self._listener = None
        _outside_refs.remove(self)
    
    def add_data_available_callback(self, cb):
        '''Warning: callback is called back in another thread!'''
        if not self._callbacks:
            self._enable_listener()
        ref = max(self._callbacks) if self._callbacks else 0
        self._callbacks[ref] = cb
        return ref
    
    def remove_data_available_callback(self, ref):
        del self._callbacks[ref]
        if not self._callbacks:
            self._disable_listener()
    
    def _data_available_callback(self, listener_data, datareader):
        for cb in self._callbacks.values():
            cb()

    def read(self):
        return self._receive(False)

    def take(self):
        return self._receive(True)

    def _receive(self, take = True):
        """'takeFlag' controls whether read samples stay in the DDS cache (i.e. use DDS Read API) or removed (i.e. use DDS Take API) """
        data_seq = DDSType.DynamicDataSeq()
        DDSFunc.DynamicDataSeq_initialize(data_seq)
        info_seq = DDSType.SampleInfoSeq()
        DDSFunc.SampleInfoSeq_initialize(info_seq)

        try:
            if take:
                self._dyn_narrowed_reader.take(ctypes.byref(data_seq), ctypes.byref(info_seq), DDS_LENGTH_UNLIMITED, get('ANY_SAMPLE_STATE', DDS_SampleStateMask), get('ANY_VIEW_STATE', DDS_ViewStateMask), get('ANY_INSTANCE_STATE', DDS_InstanceStateMask))
            else:
                self._dyn_narrowed_reader.read(ctypes.byref(data_seq), ctypes.byref(info_seq), DDS_LENGTH_UNLIMITED, get('ANY_SAMPLE_STATE', DDS_SampleStateMask), get('ANY_VIEW_STATE', DDS_ViewStateMask), get('ANY_INSTANCE_STATE', DDS_InstanceStateMask))
        except Error as e:
            if str(e) == 'no data':
                return []
            else:
                raise e
        data_seq_length = data_seq.get_length()
        samplesList = []
        try:
            for i in range(data_seq_length):
                sampleInfo = unpack_sampleInfo(info_seq.get_reference(i))
                sampleData = unpack_dd(data_seq.get_reference(i))
                sampleDict = {'sampleInfo': sampleInfo, 'sampleData': sampleData}            
                samplesList.append(sampleDict)
            return samplesList
        finally:
            self._dyn_narrowed_reader.return_loan(ctypes.byref(data_seq), ctypes.byref(info_seq))
        
class DDS(object):
    """Creating application via configuration file name (i.e. XML Application Creation)"""
    def __init__(self, configuration_name, configuration_file = None):
        if configuration_file:
            os.environ['NDDS_QOS_PROFILES'] = configuration_file

        self.configuration_name = configuration_name
        self._participant = DDSFunc.DomainParticipantFactory_get_instance().create_participant_from_config(cstring(self.configuration_name))

    def __del__(self):
        self._participant.delete_contained_entities()

    def lookup_datawriter_by_name(self, datawriter_full_name):
        """Retrieves the DDS DataWriter according to its full name (e.g. MyPublisher::HelloWorldWriter"""
        res = Writer(self,cstring(datawriter_full_name))
        return res

    def lookup_datareader_by_name(self, datareader_full_name):
        """Retrieves the DDS DataReader according to its full name (e.g. MySubscriber::HelloWorldReader"""
        res = Reader(self,cstring(datareader_full_name))
        return res
