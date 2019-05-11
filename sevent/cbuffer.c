#include <Python.h>
#include <structmember.h>
#ifdef __VMS
#   include <socket.h>
# else
#   include <sys/socket.h>
# endif

#define CHECK_ERRNO(expected) \
    (errno == expected)

typedef struct BufferQueue{
    struct BufferQueue* next;
    PyBytesObject* buffer;
    u_int8_t flag;
} BufferQueue;

#define BUFFER_QUEUE_FAST_BUFFER_COUNT 1024

static BufferQueue* buffer_queue_fast_buffer[BUFFER_QUEUE_FAST_BUFFER_COUNT];
static short buffer_queue_fast_buffer_index = 0;

#define BYTES_FAST_BUFFER_COUNT 128

static PyBytesObject* bytes_fast_buffer[BYTES_FAST_BUFFER_COUNT];
static short bytes_fast_buffer_index = 0;

static int socket_recv_size = 8192 - sizeof(PyBytesObject);

#define BufferQueue_malloc() buffer_queue_fast_buffer_index > 0 ? buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index] : (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue))
#define BufferQueue_free(buffer_queue) if(buffer_queue_fast_buffer_index < BUFFER_QUEUE_FAST_BUFFER_COUNT) { \
    buffer_queue->next = NULL;  \
    buffer_queue->buffer = NULL;    \
    buffer_queue->flag = 0; \
    buffer_queue_fast_buffer[buffer_queue_fast_buffer_index++]=buffer_queue; \
} else { \
    PyMem_Free(buffer_queue); \
}

#define PyBytesObject_malloc(size) (PyBytesObject*)PyBytes_FromStringAndSize(0, size)
#define PyBytesObject_free(objbytes, buffer_queue) if(buffer_queue->flag == 0x01 && bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT){ \
    objbytes->ob_shash = -1; \
    Py_SIZE(objbytes) = 0; \
    bytes_fast_buffer[bytes_fast_buffer_index++]=objbytes; \
} else { \
    Py_DECREF(objbytes); \
}

typedef struct {
    PyObject_VAR_HEAD
    int buffer_offset;
    BufferQueue* buffer_head;
    BufferQueue* buffer_tail;
} BufferObject;

int join_impl(BufferObject *objbuf)
{

    BufferQueue* last_queue;
    PyBytesObject* buffer;
    char* ob_sval;
    int buf_len;

    if(Py_SIZE(objbuf) == 0){
        return 0;
    }

    if(objbuf->buffer_offset == 0) {
        if (objbuf->buffer_head == objbuf->buffer_tail) {
            return 0;
        }

        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, Py_SIZE(objbuf));
        if (buffer == NULL)
            return -1;

        ob_sval = buffer->ob_sval;
    } else {
        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, Py_SIZE(objbuf));
        if (buffer == NULL)
            return -1;

        ob_sval = buffer->ob_sval;
        buf_len = Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset;
        memcpy(ob_sval, ((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval + objbuf->buffer_offset, buf_len);
        ob_sval += buf_len;
        objbuf->buffer_offset = 0;

        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }

    while(objbuf->buffer_head != NULL){
        buf_len = Py_SIZE(objbuf->buffer_head->buffer);
        memcpy(ob_sval, ((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval, buf_len);
        ob_sval += buf_len;

        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }

    BufferQueue* queue;
    if(buffer_queue_fast_buffer_index > 0) {
        queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
    } else {
        queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
        if(queue == NULL) {
            Py_DECREF(buffer);
            return -1;
        }
        queue->flag = 0;
        queue->next = NULL;
    }

    buffer->ob_sval[Py_SIZE(objbuf)] = '\0';
    queue->buffer = buffer;
    objbuf->buffer_head = queue;
    objbuf->buffer_tail = queue;
    return 0;
}

static void
Buffer_dealloc(BufferObject* self) {
    BufferObject* objbuf = (BufferObject*)self;

    BufferQueue* last_queue;
    while (objbuf->buffer_head != NULL) {
        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }
    objbuf->buffer_tail = NULL;
    Py_SIZE(objbuf) = 0;
    objbuf->buffer_offset = 0;
    self->ob_type->tp_free((PyObject*)self);
}

static PyObject*
Buffer_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    BufferObject* self;
    self = (BufferObject *)type->tp_alloc(type, 0);
    return (PyObject*) self;
}

static int
Buffer_init(BufferObject* self, PyObject* args, PyObject* kwds) {
    self->buffer_offset = 0;
    self->buffer_head = NULL;
    self->buffer_tail = NULL;
    return 0;
}

static PyObject *
Buffer_slice(register BufferObject *objbuf, register Py_ssize_t i, register Py_ssize_t j){
    if (join_impl(objbuf) != 0) {
        return PyErr_NoMemory();
    }

    if (i < 0)
        i = 0;
    if (j < 0)
        j = 0; /* Avoid signed/unsigned bug in next line */
    if (j > Py_SIZE(objbuf))
        j = Py_SIZE(objbuf);
    if (i == 0 && j == Py_SIZE(objbuf)) {
        /* It's the same as a */
        PyObject* data = (PyObject*)objbuf->buffer_head->buffer;
        Py_INCREF(data);
        return data;
    }
    if (j < i)
        j = i;
    return PyString_FromStringAndSize(((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval + i, j-i);
}

static PyObject *
Buffer_item(BufferObject *objbuf, register Py_ssize_t i)
{
    if (join_impl(objbuf) != 0) {
        return PyErr_NoMemory();
    }
    
    PyObject *v;
    if (i < 0 || i >= Py_SIZE(objbuf)) {
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }

    return PyInt_FromLong(((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval[i]);
}

static int
Buffer_nonzero(register BufferObject *objbuf) {
    return Py_SIZE(objbuf);
}

static long
Buffer_hash(register BufferObject *objbuf) {
    if (join_impl(objbuf) != 0) {
        return -1;
    }

    return PyObject_Hash((PyObject*)objbuf->buffer_head->buffer);
}

static PyObject *
Buffer_string(register BufferObject *objbuf) {
    if (join_impl(objbuf) != 0) {
        return PyErr_NoMemory();
    }

    return PyObject_Str((PyObject*)objbuf->buffer_head->buffer);
}

static PyObject *
Buffer_write(register BufferObject *objbuf, PyObject *args)
{
    PyObject* data;
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }

    if(!PyBytes_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a bytes");
        return NULL;
    }

    BufferQueue* queue;
    if(buffer_queue_fast_buffer_index > 0) {
        queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
    } else {
        queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
        if(queue == NULL) {
            return PyErr_NoMemory();
        }
        queue->flag = 0;
        queue->next = NULL;
    }
    queue->buffer = (PyBytesObject*)data;
    Py_INCREF(data);

    if(objbuf->buffer_tail == NULL) {
        objbuf->buffer_head = queue;
        objbuf->buffer_tail = queue;
    } else {
        objbuf->buffer_tail->next = queue;
        objbuf->buffer_tail = queue;
    }
    Py_SIZE(objbuf) += Py_SIZE(data);
    Py_RETURN_NONE;
}

static PyObject *
Buffer_read(register BufferObject *objbuf, PyObject *args)
{
    int read_size = -1;
    if (!PyArg_ParseTuple(args, "|i", &read_size)) {
        return NULL;
    }

    PyObject* data;

    if(read_size < 0){
        if(Py_SIZE(objbuf) == 0) {
            return PyBytes_FromStringAndSize(0, 0);
        }

        if(objbuf->buffer_offset > 0 || objbuf->buffer_head != objbuf->buffer_tail) {
            if (join_impl(objbuf) != 0) {
                return PyErr_NoMemory();
            }
        }

        data = (PyObject*)objbuf->buffer_head->buffer;
        Py_SIZE(objbuf) = 0;
        BufferQueue_free(objbuf->buffer_head);
        objbuf->buffer_head = NULL;
        objbuf->buffer_tail = NULL;
        return data;
    }

    if(Py_SIZE(objbuf) < read_size) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    PyBytesObject* buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, read_size);
    if (buffer == NULL)
        return PyErr_NoMemory();

    BufferQueue* last_queue;
    int buffer_size = 0;
    int buf_len;

    if(objbuf->buffer_offset > 0) {
        buf_len = Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset;
        buf_len = buf_len > read_size - buffer_size ? read_size - buffer_size : buf_len;
        memcpy(buffer->ob_sval + buffer_size, ((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval + objbuf->buffer_offset, buf_len);
        objbuf->buffer_offset += buf_len;
        buffer_size += buf_len;
        Py_SIZE(objbuf) -= buf_len;

        if(objbuf->buffer_offset >= Py_SIZE(objbuf->buffer_head->buffer)) {
            objbuf->buffer_offset = 0;
            last_queue = objbuf->buffer_head;
            objbuf->buffer_head = objbuf->buffer_head->next;
            PyBytesObject_free(last_queue->buffer, last_queue);
            BufferQueue_free(last_queue);
            if(objbuf->buffer_head == NULL) {
                objbuf->buffer_tail = NULL;
            }
        }
    }

    while(buffer_size < read_size) {
        buf_len = Py_SIZE(objbuf->buffer_head->buffer);
        buf_len = buf_len > read_size - buffer_size ? read_size - buffer_size : buf_len;
        memcpy(buffer->ob_sval + buffer_size, ((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval, buf_len);
        objbuf->buffer_offset += buf_len;
        buffer_size += buf_len;
        Py_SIZE(objbuf) -= buf_len;

        if(objbuf->buffer_offset >= Py_SIZE(objbuf->buffer_head->buffer)) {
            objbuf->buffer_offset = 0;
            last_queue = objbuf->buffer_head;
            objbuf->buffer_head = objbuf->buffer_head->next;
            PyBytesObject_free(last_queue->buffer, last_queue);
            BufferQueue_free(last_queue);
            if(objbuf->buffer_head == NULL) {
                objbuf->buffer_tail = NULL;
            }
        }
    }

    buffer->ob_sval[read_size] = '\0';
    return (PyObject*)buffer;
}

static PyObject *
Buffer_next(register BufferObject *objbuf, PyObject *args) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    BufferQueue* last_queue;
    PyBytesObject* buffer;
    if(objbuf->buffer_offset > 0) {
        int buf_size = Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset;
        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
        if (buffer == NULL)
            return PyErr_NoMemory();

        memcpy(buffer->ob_sval, ((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval + objbuf->buffer_offset, buf_size);
        Py_SIZE(objbuf) -= buf_size;
        objbuf->buffer_offset = 0;
        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
        if(objbuf->buffer_head == NULL) {
            objbuf->buffer_tail = NULL;
        }

        buffer->ob_sval[buf_size] = '\0';
        return (PyObject *)buffer;
    }

    buffer = (PyBytesObject*)objbuf->buffer_head->buffer;
    Py_SIZE(objbuf) -= Py_SIZE(buffer);
    last_queue = objbuf->buffer_head;
    objbuf->buffer_head = objbuf->buffer_head->next;
    BufferQueue_free(last_queue);
    if(objbuf->buffer_head == NULL) {
        objbuf->buffer_tail = NULL;
    }
    return (PyObject *)buffer;
}

static PyObject *
Buffer_join(register BufferObject *objbuf, PyObject *args) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    if(join_impl((BufferObject*) objbuf) != 0) {
        return PyErr_NoMemory();
    }

    PyObject* data = (PyObject*)objbuf->buffer_head->buffer;
    Py_INCREF(data);
    return data;
}

static Py_ssize_t
Buffer_length(register BufferObject *objbuf)
{
    return Py_SIZE(objbuf);
}

static PyObject *
Buffer_socket_recv(register BufferObject *objbuf, PyObject *args)
{
    int sock_fd;
    if (!PyArg_ParseTuple(args, "i", &sock_fd)) {
        return NULL;
    }

    PyBytesObject* buf;
    int result = 0;
    int recv_len = 0;

    while (1) {
        if(bytes_fast_buffer_index > 0) {
            buf = bytes_fast_buffer[--bytes_fast_buffer_index];
        } else {
            buf = (PyBytesObject*)PyBytes_FromStringAndSize(0, socket_recv_size);
            if(buf == NULL) {
                return PyErr_NoMemory();
            }
        }

        result = recv(sock_fd, buf->ob_sval, socket_recv_size, 0);
        if(result < 0) {
            if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
            } else {
                Py_DECREF(buf);
            }
            if(CHECK_ERRNO(EWOULDBLOCK) || CHECK_ERRNO(EAGAIN)) {
                return PyInt_FromLong(recv_len);
            }
            return PyErr_SetFromErrno(PyExc_OSError);
        }

        if(result == 0) {
            if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
            } else {
                Py_DECREF(buf);
            }
            return PyInt_FromLong(recv_len);
        }

        Py_SIZE(buf) = result;

        BufferQueue* queue;
        if(buffer_queue_fast_buffer_index > 0) {
            queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
        } else {
            queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
            if(queue == NULL) {
                return PyErr_NoMemory();
            }
            queue->flag = 0;
            queue->next = NULL;
        }
        buf->ob_sval[result] = '\0';
        queue->buffer = buf;

        if(objbuf->buffer_tail == NULL) {
            objbuf->buffer_head = queue;
            objbuf->buffer_tail = queue;
        } else {
            objbuf->buffer_tail->next = queue;
            objbuf->buffer_tail = queue;
        }
        Py_SIZE(objbuf) += result;
        recv_len += result;
    }
}

static PyObject *
Buffer_socket_send(register BufferObject *objbuf, PyObject *args)
{
    int sock_fd;
    if (!PyArg_ParseTuple(args, "i", &sock_fd)) {
        return NULL;
    }

    int result = 0;
    int send_len = 0;
    BufferQueue* last_queue;

    while (objbuf->buffer_head != NULL) {
        result = send(sock_fd, ((PyBytesObject*)objbuf->buffer_head->buffer)->ob_sval + objbuf->buffer_offset, Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset, 0);
        if(result < 0) {
            if(CHECK_ERRNO(EWOULDBLOCK) || CHECK_ERRNO(EAGAIN)) {
                return PyInt_FromLong(send_len);
            }
            return PyErr_SetFromErrno(PyExc_OSError);
        }

        objbuf->buffer_offset += result;
        Py_SIZE(objbuf) -= result;
        send_len += result;
        if(objbuf->buffer_offset >= Py_SIZE(objbuf->buffer_head->buffer)) {
            objbuf->buffer_offset = 0;
            last_queue = objbuf->buffer_head;
            objbuf->buffer_head = objbuf->buffer_head->next;
            PyBytesObject_free(last_queue->buffer, last_queue);
            BufferQueue_free(last_queue);
            if(objbuf->buffer_head == NULL) {
                objbuf->buffer_tail = NULL;
            }
        }
    }
    return PyInt_FromLong(send_len);
}

static PyObject*
Buffer_buffer_getter(register BufferObject *objbuf, void *args) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    PyObject* data = (PyObject*)objbuf->buffer_head->buffer;
    Py_INCREF(data);
    return data;
}

static PyObject*
Buffer_buffer_len_getter(register BufferObject *objbuf, void *args) {
    if(Py_SIZE(objbuf) == 0) {
        return PyInt_FromLong(0);
    }

    return PyInt_FromLong(Py_SIZE(objbuf->buffer_head->buffer));
}

static PyObject*
Buffer_buffers_getter(register BufferObject *objbuf, void *args) {
    PyObject* buffers = PyList_New(0);

    if(Py_SIZE(objbuf) == 0) {
        return buffers;
    }

    BufferQueue *current = objbuf->buffer_head;
    while (current != NULL) {
        PyList_Append(buffers, (PyObject*)current->buffer);
        current = current->next;
    }

    return buffers;
}

static PyMemberDef Buffer_members[] = {
        {"_buffer_index", T_INT, offsetof(BufferObject, buffer_offset), READONLY, "buffer_offset"},
        {"_len", T_INT, offsetof(BufferObject, ob_size), READONLY, "buffer_len"},
        {NULL}  /* Sentinel */
};

static PyGetSetDef Buffer_getseters[] = {
        {"_buffer", (getter)Buffer_buffer_getter, 0, "_buffer", 0},
        {"_buffer_len", (getter)Buffer_buffer_len_getter, 0, "_buffer_len", 0},
        {"_buffers", (getter)Buffer_buffers_getter, 0, "_buffers", 0},
        {NULL} /* Sentinel */
};

static PyMethodDef Buffer_methods[] = {
        {"write", (PyCFunction)Buffer_write, METH_VARARGS, "buffer write"},
        {"read", (PyCFunction)Buffer_read, METH_VARARGS, "buffer read"},
        {"join", (PyCFunction)Buffer_join, METH_VARARGS, "buffer join"},
        {"next", (PyCFunction)Buffer_next, METH_VARARGS, "buffer next"},
        {"socket_send", (PyCFunction)Buffer_socket_send, METH_VARARGS, "buffer socket_send"},
        {"socket_recv", (PyCFunction)Buffer_socket_recv, METH_VARARGS, "buffer socket_recv"},
        {NULL}  /* Sentinel */
};

static PySequenceMethods Buffer_as_sequence = {
        (lenfunc)Buffer_length, /*sq_length*/
        0,                      /*sq_concat*/
        0,                      /*sq_repeat*/
        (ssizeargfunc)Buffer_item,                      /*sq_item*/
        (ssizessizeargfunc)Buffer_slice,                      /*sq_slice*/
        0,                      /*sq_ass_item*/
        0,                      /*sq_ass_slice*/
        0                       /*sq_contains*/
};

static PyNumberMethods Buffer_as_number = {
        0, //nb_add;
        0, //nb_subtract;
        0, //nb_multiply;
        0, //nb_divide;
        0, //nb_remainder;
        0, //nb_divmod;
        0, //nb_power;
        0, //nb_negative;
        0, //nb_positive;
        0, //nb_absolute;
        (inquiry)Buffer_nonzero, //nb_nonzero;
};

static PyTypeObject BufferType = {
        PyVarObject_HEAD_INIT(&PyType_Type, 0)    /*ob_size*/
        "cbuffer.Buffer",                         /*tp_name*/
        sizeof(BufferObject),                     /*tp_basicsize*/
        0,                                        /*tp_itemsize*/
        (destructor)Buffer_dealloc,               /*tp_dealloc*/
        0,                                        /*tp_print*/
        0,                                        /*tp_getattr*/
        0,                                        /*tp_setattr*/
        0,                                        /*tp_compare*/
        0,                                        /*tp_repr*/
        &Buffer_as_number,                        /*tp_as_number*/
        &Buffer_as_sequence,                      /*tp_as_sequence*/
        0,                                        /*tp_as_mapping*/
        (hashfunc)Buffer_hash,                    /*tp_hash */
        0,                                        /*tp_call*/
        (reprfunc)Buffer_string,                  /*tp_str*/
        0,                                        /*tp_getattro*/
        0,                                        /*tp_setattro*/
        0,                                        /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
        "Buffer objects",                         /* tp_doc */
        0,                                        /* tp_traverse */
        0,                                        /* tp_clear */
        0,                                        /* tp_richcompare */
        0,                                        /* tp_weaklistoffset */
        0,                                        /* tp_iter */
        0,                                        /* tp_iternext */
        Buffer_methods,                           /* tp_methods */
        Buffer_members,                           /* tp_members */
        Buffer_getseters,                         /* tp_getset */
        &PyBaseObject_Type,                       /* tp_base */
        0,                                        /* tp_dict */
        0,                                        /* tp_descr_get */
        0,                                        /* tp_descr_set */
        0,                                        /* tp_dictoffset */
        (initproc)Buffer_init,                    /* tp_init */
        0,                                        /* tp_alloc */
        Buffer_new,                               /* tp_new */
};


static PyObject *
cbuffer_socket_recv(PyObject *self, PyObject *args) {
    int sock_fd;
    if (!PyArg_ParseTuple(args, "i", &sock_fd)) {
        return NULL;
    }

    PyBytesObject* buf;
    if(bytes_fast_buffer_index > 0) {
        buf = bytes_fast_buffer[--bytes_fast_buffer_index];
    } else {
        buf = (PyBytesObject*)PyBytes_FromStringAndSize(0, socket_recv_size);
        if(buf == NULL) {
            return PyErr_NoMemory();
        }
    }

    int result = recv(sock_fd, buf->ob_sval, socket_recv_size, 0);
    if(result < 0) {
        if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
            bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
        } else {
            Py_DECREF(buf);
        }
        return PyErr_SetFromErrno(PyExc_OSError);
    }

    Py_SIZE(buf) = result;
    buf->ob_sval[result] = '\0';
    return (PyObject *)buf;
}

static PyObject *
cbuffer_socket_send(PyObject *self, PyObject *args) {
    int sock_fd;
    PyObject* data;
    if (!PyArg_ParseTuple(args, "iO", &sock_fd, &data)) {
        return NULL;
    }

    if(!PyBytes_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a bytes");
        return NULL;
    }

    int result = send(sock_fd, ((PyBytesObject*)data)->ob_sval, Py_SIZE(data), 0);
    if(result < 0) {
        return PyErr_SetFromErrno(PyExc_OSError);;
    }

    return PyInt_FromLong(result);
}

static PyObject *
cbuffer_socket_set_recv_size(PyObject *self, PyObject *args) {
    int recv_size;
    if (!PyArg_ParseTuple(args, "i", &recv_size)) {
        return NULL;
    }

    socket_recv_size = recv_size;
    Py_RETURN_NONE;
}

static PyObject *
cbuffer_socket_get_recv_size(PyObject *self, PyObject *args) {
    return PyInt_FromLong(socket_recv_size);
}

static PyMethodDef module_methods[] =
{
        {"socket_send", (PyCFunction)cbuffer_socket_send, METH_VARARGS, "socket_send"},
        {"socket_recv", (PyCFunction)cbuffer_socket_recv, METH_VARARGS, "socket_recv"},
        {"socket_set_recv_size", (PyCFunction)cbuffer_socket_set_recv_size, METH_VARARGS, "socket_set_recv_size"},
        {"socket_get_recv_size", (PyCFunction)cbuffer_socket_get_recv_size, METH_VARARGS, "socket_get_recv_size"},
        {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initcbuffer()
{
    PyObject* m;
    if (PyType_Ready(&BufferType) < 0) {
        return;
    }

    m = Py_InitModule3("cbuffer", module_methods, "cbuffer");
    if (m == NULL) {
        return;
    }

    PyModule_AddObject(m, "Buffer", (PyObject*)&BufferType);

    int init_buffer_queue_fast_buffer_count = BUFFER_QUEUE_FAST_BUFFER_COUNT / 3;
    while (buffer_queue_fast_buffer_index < init_buffer_queue_fast_buffer_count) {
        BufferQueue* buffer_queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
        if(buffer_queue == NULL) {
            return;
        }

        buffer_queue->next = NULL;
        buffer_queue->flag = 0;
        buffer_queue->buffer = NULL;
        buffer_queue_fast_buffer[buffer_queue_fast_buffer_index] = buffer_queue;
        buffer_queue_fast_buffer_index++;
    }
}

PyMODINIT_FUNC
PyInit_cbuffer() {
    return initcbuffer();
}