#include <Python.h>
#include <structmember.h>
#ifndef MS_WINDOWS
#ifdef __VMS
#include <socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif
#else /* MS_WINDOWS */
#include <winsock2.h>
#include <ws2ipdef.h>
typedef int socklen_t;
#endif

#if PY_MAJOR_VERSION >= 3
#define PyInt_FromLong PyLong_FromLong
#define PyInt_CheckExact PyLong_CheckExact
#define PyInt_AS_LONG PyLong_AsLong

#define PyString_FromString PyUnicode_FromString
#define PyString_CheckExact PyUnicode_CheckExact
#define PyStringB_CheckExact PyBytes_CheckExact
#else
#define PyStringB_CheckExact PyUnicode_CheckExact
typedef long Py_hash_t;
#endif

#if PY_VERSION_HEX < 0x030900A4 && !defined(Py_SET_SIZE)
#define Py_SET_SIZE(ob, size) ((PyVarObject*)(ob))->ob_size = size
#endif

#ifdef MS_WINDOWS
#ifndef WSAEAGAIN
#define WSAEAGAIN WSAEWOULDBLOCK
#endif
#define CHECK_ERRNO(expected) \
    (WSAGetLastError() == WSA ## expected)
#else
#define CHECK_ERRNO(expected) \
    (errno == expected)
#endif

static PyObject *
set_error(void)
{
#ifdef MS_WINDOWS
    int err_no = WSAGetLastError();
    /* PyErr_SetExcFromWindowsErr() invokes FormatMessage() which
       recognizes the error codes used by both GetLastError() and
       WSAGetLastError */
    if (err_no)
        return PyErr_SetExcFromWindowsErr(PyExc_OSError, err_no);
#endif

    return PyErr_SetFromErrno(PyExc_OSError);
}

typedef struct BufferQueue{
    struct BufferQueue* next;
    PyBytesObject* buffer;
    PyObject* odata;
    unsigned char flag;
} BufferQueue;

#define BUFFER_QUEUE_FAST_BUFFER_COUNT 1024

static BufferQueue* buffer_queue_fast_buffer[BUFFER_QUEUE_FAST_BUFFER_COUNT];
static short buffer_queue_fast_buffer_index = 0;

#define BYTES_FAST_BUFFER_COUNT 128

static PyBytesObject* bytes_fast_buffer[BYTES_FAST_BUFFER_COUNT];
static short bytes_fast_buffer_index = 0;

static int socket_recv_size = 8192 - sizeof(PyBytesObject);
static int socket_recv_count = 8;
static int socket_send_count = 8;

#define BufferQueue_malloc() buffer_queue_fast_buffer_index > 0 ? buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index] : (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue))
#define BufferQueue_free(buffer_queue) if(buffer_queue_fast_buffer_index < BUFFER_QUEUE_FAST_BUFFER_COUNT) { \
    buffer_queue->next = NULL;  \
    buffer_queue->buffer = NULL;    \
    if(buffer_queue->odata != NULL) { \
        Py_DECREF(buffer_queue->odata); \
        buffer_queue->odata = NULL; \
    } \
    buffer_queue->flag = 0; \
    buffer_queue_fast_buffer[buffer_queue_fast_buffer_index++]=buffer_queue; \
} else { \
    if(buffer_queue->odata != NULL) { \
        Py_DECREF(buffer_queue->odata); \
    } \
    PyMem_Free(buffer_queue); \
}

#define PyBytesObject_malloc(size) (PyBytesObject*)PyBytes_FromStringAndSize(0, size)
#define PyBytesObject_free(objbytes, buffer_queue) if(buffer_queue->flag == 0x01 && bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT){ \
    objbytes->ob_shash = -1; \
    Py_SET_SIZE(objbytes, 0); \
    bytes_fast_buffer[bytes_fast_buffer_index++]=objbytes; \
} else { \
    Py_DECREF(objbytes); \
}

static char sock_addr_host[64];

typedef struct {
    PyObject_VAR_HEAD
    Py_ssize_t buffer_offset;
    BufferQueue* buffer_head;
    BufferQueue* buffer_tail;
} BufferObject;

int join_impl(register BufferObject *objbuf)
{

    BufferQueue* last_queue;
    PyBytesObject* buffer;
    PyObject* odata;
    char* ob_sval;
    Py_ssize_t buf_len;

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
        odata = objbuf->buffer_tail->odata;
        if(odata != NULL) {
            Py_INCREF(odata);
        }
    } else {
        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, Py_SIZE(objbuf));
        if (buffer == NULL)
            return -1;

        ob_sval = buffer->ob_sval;
        odata = objbuf->buffer_tail->odata;
        if(odata != NULL) {
            Py_INCREF(odata);
        }

        buf_len = Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset;
        memcpy(ob_sval, objbuf->buffer_head->buffer->ob_sval + objbuf->buffer_offset, buf_len);
        ob_sval += buf_len;
        objbuf->buffer_offset = 0;

        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }

    while(objbuf->buffer_head != NULL){
        buf_len = Py_SIZE(objbuf->buffer_head->buffer);
        memcpy(ob_sval, objbuf->buffer_head->buffer->ob_sval, buf_len);
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
            if(odata != NULL) {
                Py_DECREF(odata);
            }
            objbuf->buffer_tail = NULL;
            Py_SET_SIZE(objbuf, 0);
            return -1;
        }
        queue->flag = 0;
        queue->next = NULL;
    }

    queue->buffer = buffer;
    queue->odata = odata;
    objbuf->buffer_head = queue;
    objbuf->buffer_tail = queue;
    return 0;
}

static void
Buffer_dealloc(register BufferObject* objbuf) {
    BufferQueue* last_queue;
    while (objbuf->buffer_head != NULL) {
        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }
    objbuf->buffer_tail = NULL;
    Py_SET_SIZE(objbuf, 0);
    objbuf->buffer_offset = 0;
    ((PyObject*)objbuf)->ob_type->tp_free((PyObject*)objbuf);
}

static PyObject*
Buffer_new(register PyTypeObject *type, PyObject *args, PyObject *kwds) {
    BufferObject* objbuf;
    objbuf = (BufferObject *)type->tp_alloc(type, 0);
    return (PyObject*) objbuf;
}

static int
Buffer_init(register BufferObject* objbuf, PyObject* args, PyObject* kwds) {
    objbuf->buffer_offset = 0;
    objbuf->buffer_head = NULL;
    objbuf->buffer_tail = NULL;
    return 0;
}

static PyObject *
Buffer_slice(register BufferObject *objbuf, register Py_ssize_t i, register Py_ssize_t j){
    if (Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    if (join_impl(objbuf) != 0) {
        return PyErr_NoMemory();
    }

    if (i < 0)
        i = 0;
    if (j < 0)
        j = 0;
    if (j > Py_SIZE(objbuf))
        j = Py_SIZE(objbuf);
    if (i == 0 && j == Py_SIZE(objbuf)) {
        PyObject* data = (PyObject*)objbuf->buffer_head->buffer;
        if(objbuf->buffer_head->odata != NULL) {
            return PyTuple_Pack(2, data, objbuf->buffer_head->odata);
        }
        Py_INCREF(data);
        return data;
    }
    if (j < i)
        j = i;
    if(objbuf->buffer_head->odata != NULL) {
        return PyTuple_Pack(2, PyBytes_FromStringAndSize(objbuf->buffer_head->buffer->ob_sval + i, j-i), objbuf->buffer_head->odata);
    }
    return PyBytes_FromStringAndSize(objbuf->buffer_head->buffer->ob_sval + i, j-i);
}

static PyObject *
Buffer_item(register BufferObject *objbuf, register Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(objbuf)) {
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }

    if (join_impl(objbuf) != 0) {
        return PyErr_NoMemory();
    }

    if(objbuf->buffer_head->odata != NULL) {
        return PyTuple_Pack(2, PyInt_FromLong(objbuf->buffer_head->buffer->ob_sval[i]),
                            objbuf->buffer_head->odata);
    }
#if PY_MAJOR_VERSION >= 3
    return PyInt_FromLong(objbuf->buffer_head->buffer->ob_sval[i]);
#else
    return PyBytes_FromStringAndSize(objbuf->buffer_head->buffer->ob_sval + i, 1);
#endif
}

static int
Buffer_nonzero(register BufferObject *objbuf) {
    return (int)Py_SIZE(objbuf);
}

static Py_hash_t
Buffer_hash(register BufferObject *objbuf) {
    if (Py_SIZE(objbuf) == 0) {
        return PyObject_Hash(PyBytes_FromStringAndSize(0, 0));
    }

    if (join_impl(objbuf) != 0) {
        return -1;
    }

    return PyObject_Hash((PyObject*)objbuf->buffer_head->buffer);
}

static PyObject *
Buffer_string(register BufferObject *objbuf) {
    if (Py_SIZE(objbuf) == 0) {
        return PyObject_Str(PyBytes_FromStringAndSize(0, 0));
    }

    if (join_impl(objbuf) != 0) {
        return PyErr_NoMemory();
    }

    return PyObject_Str((PyObject*)objbuf->buffer_head->buffer);
}

#if PY_MAJOR_VERSION < 3
static Py_ssize_t
Buffer_getreadbuf(BufferObject *objbuf, Py_ssize_t index, const void **ptr)
{
    if ( index != 0 ) {
        PyErr_SetString(PyExc_SystemError, "accessing non-existent string segment");
        return -1;
    }

    if (Py_SIZE(objbuf) == 0) {
        PyErr_SetString(PyExc_IndexError, "buffer empty");
        return -1;
    }

    if (join_impl(objbuf) != 0) {
        PyErr_SetString(PyExc_MemoryError, "out of memory");
        return -1;
    }

    *ptr = objbuf->buffer_head->buffer->ob_sval;
    return Py_SIZE(objbuf);
}

static Py_ssize_t
Buffer_getwritebuf(BufferObject *objbuf, Py_ssize_t index, const void **ptr)
{
    PyErr_SetString(PyExc_TypeError,
                    "Cannot use string as modifiable buffer");
    return -1;
}

static Py_ssize_t
Buffer_getsegcount(BufferObject *objbuf, Py_ssize_t *lenp)
{
    if (Py_SIZE(objbuf) == 0) {
        PyErr_SetString(PyExc_IndexError, "buffer empty");
        return -1;
    }

    if (join_impl(objbuf) != 0) {
        PyErr_SetString(PyExc_MemoryError, "out of memory");
        return -1;
    }

    if ( lenp )
        *lenp = Py_SIZE(objbuf);
    return 1;
}

static Py_ssize_t
Buffer_getcharbuf(BufferObject *objbuf, Py_ssize_t index, const char **ptr)
{
    if ( index != 0 ) {
        PyErr_SetString(PyExc_SystemError, "accessing non-existent string segment");
        return -1;
    }

    if (Py_SIZE(objbuf) == 0) {
        PyErr_SetString(PyExc_IndexError, "buffer empty");
        return -1;
    }

    if (join_impl(objbuf) != 0) {
        PyErr_SetString(PyExc_MemoryError, "out of memory");
        return -1;
    }
    *ptr = objbuf->buffer_head->buffer->ob_sval;
    return Py_SIZE(objbuf);
}
#endif

static int
Buffer_getbuffer(BufferObject *objbuf, Py_buffer *view, int flags)
{

    if (Py_SIZE(objbuf) == 0) {
        PyErr_SetString(PyExc_IndexError, "buffer empty");
        return -1;
    }

    if (join_impl(objbuf) != 0) {
        PyErr_SetString(PyExc_MemoryError, "out of memory");
        return -1;
    }

    return PyBuffer_FillInfo(view, (PyObject*)objbuf->buffer_head->buffer,
            objbuf->buffer_head->buffer->ob_sval, Py_SIZE(objbuf->buffer_head->buffer), 1, flags);
}

static PyObject *
Buffer_write(register BufferObject *objbuf, PyObject *args)
{
    PyObject* data;
    PyObject* odata = NULL;
    if (!PyArg_ParseTuple(args, "O|O", &data, &odata)) {
        return NULL;
    }

    if(!PyBytes_CheckExact(data)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a bytes");
        return NULL;
    }

    if(Py_SIZE(data) <= 0) {
        Py_RETURN_NONE;
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
    queue->odata = odata;
    Py_INCREF(data);
    if(odata != NULL) {
        Py_INCREF(odata);
    }

    if(objbuf->buffer_tail == NULL) {
        objbuf->buffer_head = queue;
        objbuf->buffer_tail = queue;
    } else {
        objbuf->buffer_tail->next = queue;
        objbuf->buffer_tail = queue;
    }
    Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + Py_SIZE(data));
    Py_RETURN_NONE;
}

static PyObject *
Buffer_read(register BufferObject *objbuf, PyObject *args)
{
    int read_size = -1;
    if (!PyArg_ParseTuple(args, "|i", &read_size)) {
        return NULL;
    }

    PyBytesObject* buffer;
    PyObject* odata = NULL;
    PyObject* pdata = NULL;

    if(read_size < 0){
        if(Py_SIZE(objbuf) == 0) {
            return PyBytes_FromStringAndSize(0, 0);
        }

        if(objbuf->buffer_offset > 0 || objbuf->buffer_head != objbuf->buffer_tail) {
            if (join_impl(objbuf) != 0) {
                return PyErr_NoMemory();
            }
        }

        buffer = objbuf->buffer_head->buffer;
        odata = objbuf->buffer_head->odata;
        if(odata != NULL) {
            pdata = PyTuple_Pack(2, buffer, odata);
            Py_DECREF(buffer);
        }

        Py_SET_SIZE(objbuf, 0);
        BufferQueue_free(objbuf->buffer_head);
        objbuf->buffer_head = NULL;
        objbuf->buffer_tail = NULL;
        return odata != NULL ? pdata : (PyObject*)buffer;
    }

    if(read_size == 0 || Py_SIZE(objbuf) < read_size) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    BufferQueue* last_queue;
    Py_ssize_t buffer_size = 0;
    Py_ssize_t buf_len;

    if(objbuf->buffer_offset == 0) {
        if(read_size == Py_SIZE(objbuf->buffer_head->buffer)) {
            buffer = objbuf->buffer_head->buffer;
            odata = objbuf->buffer_head->odata;
            if(odata != NULL) {
                pdata = PyTuple_Pack(2, buffer, odata);
                Py_DECREF(buffer);
            }
            Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - read_size);

            last_queue = objbuf->buffer_head;
            objbuf->buffer_head = objbuf->buffer_head->next;
            BufferQueue_free(last_queue);
            if(objbuf->buffer_head == NULL) {
                objbuf->buffer_tail = NULL;
            }
            return odata != NULL ? pdata : (PyObject*)buffer;
        }

        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, read_size);
        if (buffer == NULL)
            return PyErr_NoMemory();
    } else {
        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, read_size);
        if (buffer == NULL)
            return PyErr_NoMemory();

        buf_len = Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset;
        buf_len = buf_len > read_size - buffer_size ? read_size - buffer_size : buf_len;
        memcpy(buffer->ob_sval + buffer_size, objbuf->buffer_head->buffer->ob_sval + objbuf->buffer_offset, buf_len);
        objbuf->buffer_offset += buf_len;
        buffer_size += buf_len;
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - buf_len);
        if(buffer_size == read_size) {
            odata = objbuf->buffer_head->odata;
            if(odata != NULL) {
                pdata = PyTuple_Pack(2, buffer, odata);
            }
        }


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
        memcpy(buffer->ob_sval + buffer_size, objbuf->buffer_head->buffer->ob_sval, buf_len);
        objbuf->buffer_offset += buf_len;
        buffer_size += buf_len;
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - buf_len);
        if(buffer_size == read_size) {
            odata = objbuf->buffer_head->odata;
            if(odata != NULL) {
                pdata = PyTuple_Pack(2, buffer, odata);
            }
        }

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

    return odata != NULL ? pdata : (PyObject*)buffer;
}

static PyObject *
Buffer_next(register BufferObject *objbuf) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    PyObject* odata;
    PyObject* pdata = NULL;
    BufferQueue* last_queue;
    PyBytesObject* buffer;
    if(objbuf->buffer_offset > 0) {
        Py_ssize_t buf_size = Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset;
        buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
        if (buffer == NULL)
            return PyErr_NoMemory();

        memcpy(buffer->ob_sval, objbuf->buffer_head->buffer->ob_sval + objbuf->buffer_offset, buf_size);
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - buf_size);
        objbuf->buffer_offset = 0;
        odata = objbuf->buffer_head->odata;
        if(odata != NULL) {
            pdata = PyTuple_Pack(2, buffer, odata);
        }

        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
        if(objbuf->buffer_head == NULL) {
            objbuf->buffer_tail = NULL;
        }

        return odata != NULL ? pdata : (PyObject*)buffer;
    }

    buffer = objbuf->buffer_head->buffer;
    odata = objbuf->buffer_head->odata;
    if(odata != NULL) {
        pdata = PyTuple_Pack(2, buffer, odata);
        Py_DECREF(buffer);
    }
    Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - Py_SIZE(buffer));
    last_queue = objbuf->buffer_head;
    objbuf->buffer_head = objbuf->buffer_head->next;
    BufferQueue_free(last_queue);
    if(objbuf->buffer_head == NULL) {
        objbuf->buffer_tail = NULL;
    }
    return odata != NULL ? pdata : (PyObject*)buffer;
}

static PyObject *
Buffer_extend(register BufferObject *objbuf, register BufferObject *databuf) {
    if (Py_TYPE(objbuf) != Py_TYPE(databuf)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a buffer");
        return NULL;
    }

    if (Py_SIZE(databuf) == 0 || objbuf == databuf) {
        Py_RETURN_NONE;
    }

    if(objbuf->buffer_head == NULL) {
        objbuf->buffer_head = databuf->buffer_head;
        objbuf->buffer_tail = databuf->buffer_tail;
        objbuf->buffer_offset = databuf->buffer_offset;
        databuf->buffer_offset = 0;
    } else {
        if(databuf->buffer_offset > 0) {
            Py_ssize_t buf_size = Py_SIZE(databuf->buffer_head->buffer) - databuf->buffer_offset;
            PyBytesObject* buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
            if (buffer == NULL)
                return PyErr_NoMemory();

            memcpy(buffer->ob_sval, databuf->buffer_head->buffer->ob_sval + databuf->buffer_offset, buf_size);
            databuf->buffer_offset = 0;
            PyBytesObject_free(databuf->buffer_head->buffer, databuf->buffer_head);
            databuf->buffer_head->buffer = buffer;
            databuf->buffer_head->flag = 0;
        }
        objbuf->buffer_tail->next = databuf->buffer_head;
        objbuf->buffer_tail = databuf->buffer_tail;
    }
    databuf->buffer_head = NULL;
    databuf->buffer_tail = NULL;
    Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + Py_SIZE(databuf));
    Py_SET_SIZE(databuf, 0);
    Py_RETURN_NONE;
}

static PyObject *
Buffer_fetch(register BufferObject *objbuf, PyObject *args) {
    PyObject* data;
    int size = -1;
    if (!PyArg_ParseTuple(args, "O|i", &data, &size)) {
        return NULL;
    }

    if (Py_TYPE(objbuf) != Py_TYPE(data)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a buffer");
        return NULL;
    }

    BufferObject *databuf = (BufferObject*)data;
    if (size == 0 || Py_SIZE(databuf) == 0 || objbuf == databuf) {
        return PyInt_FromLong(0);
    }

    if(size < 0 || size > Py_SIZE(databuf)) {
        size = Py_SIZE(databuf);
    }

    Py_ssize_t fetch_size = 0;
    BufferQueue* current_queue;
    Py_ssize_t buffer_offset;
    Py_ssize_t buf_size;
    PyBytesObject* buffer;
    while(Py_SIZE(databuf) > 0) {
        buf_size = Py_SIZE(databuf->buffer_head->buffer) - databuf->buffer_offset;
        if (fetch_size + buf_size <= size) {
            current_queue = databuf->buffer_head;
            buffer_offset = databuf->buffer_offset;
            databuf->buffer_head = databuf->buffer_head->next;
            databuf->buffer_offset = 0;
            Py_SET_SIZE(databuf, Py_SIZE(databuf) - buf_size);
            if(databuf->buffer_head == NULL) {
                databuf->buffer_tail = NULL;
            }
            current_queue->next = NULL;
        } else {
            buf_size = size - fetch_size;
            buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
            if (buffer == NULL)
                return PyErr_NoMemory();
            memcpy(buffer->ob_sval, databuf->buffer_head->buffer->ob_sval + databuf->buffer_offset, buf_size);
            databuf->buffer_offset += buf_size;
            Py_SET_SIZE(databuf, Py_SIZE(databuf) - buf_size);

            if (buffer_queue_fast_buffer_index > 0) {
                current_queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
            } else {
                current_queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
                if (current_queue == NULL) {
                    Py_DECREF(buffer);
                    return PyErr_NoMemory();
                }
                current_queue->next = NULL;
                current_queue->odata = NULL;
            }
            current_queue->flag = 0x00;
            current_queue->buffer = buffer;
            if (databuf->buffer_head->odata != NULL) {
                current_queue->odata = databuf->buffer_head->odata;
                Py_INCREF(current_queue->odata);
            }
            buffer_offset = 0;
        }
        
        if (objbuf->buffer_head == NULL) {
            objbuf->buffer_head = current_queue;
            objbuf->buffer_tail = current_queue;
            objbuf->buffer_offset = buffer_offset;
        } else {
            if (buffer_offset > 0) {
                buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
                if (buffer == NULL) {
                    PyBytesObject_free(current_queue->buffer, current_queue);
                    BufferQueue_free(current_queue);
                    return PyErr_NoMemory();
                }

                memcpy(buffer->ob_sval, current_queue->buffer->ob_sval + buffer_offset, buf_size);
                PyBytesObject_free(current_queue->buffer, current_queue);
                current_queue->buffer = buffer;
                current_queue->flag = 0x00;
            }
            objbuf->buffer_tail->next = current_queue;
            objbuf->buffer_tail = current_queue;
        }
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + buf_size);
        fetch_size += buf_size;
        if (fetch_size >= size) {
            return PyInt_FromLong((long) fetch_size);
        }
    }

    return PyInt_FromLong((long) fetch_size);
}

static PyObject *
Buffer_copyfrom(register BufferObject *objbuf, PyObject *args) {
    PyObject* data;
    int size = -1;
    if (!PyArg_ParseTuple(args, "O|i", &data, &size)) {
        return NULL;
    }

    if (Py_TYPE(objbuf) != Py_TYPE(data)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a buffer");
        return NULL;
    }

    BufferObject *databuf = (BufferObject*)data;
    if (size == 0 || Py_SIZE(databuf) == 0 || objbuf == databuf) {
        return PyInt_FromLong(0);
    }

    if(size < 0 || size > Py_SIZE(databuf)) {
        size = Py_SIZE(databuf);
    }

    Py_ssize_t copy_size = 0;
    BufferQueue* copy_queue;
    Py_ssize_t buf_size;
    PyBytesObject* buffer;
    
    BufferQueue* current_queue = databuf->buffer_head;
    Py_ssize_t buffer_offset = databuf->buffer_offset;
    while(current_queue != NULL) {
        buf_size = Py_SIZE(current_queue->buffer) - buffer_offset;
        if (copy_size + buf_size <= size) {
            buffer = current_queue->buffer;
            Py_INCREF(current_queue->buffer);
        } else {
            buf_size = size - copy_size;
            buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
            if (buffer == NULL)
                return PyErr_NoMemory();
            memcpy(buffer->ob_sval, current_queue->buffer->ob_sval + buffer_offset, buf_size);
            buffer_offset = 0;
        }

        if (buffer_queue_fast_buffer_index > 0) {
            copy_queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
        } else {
            copy_queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
            if (copy_queue == NULL) {
                Py_DECREF(buffer);
                return PyErr_NoMemory();
            }
            copy_queue->next = NULL;
        }
        copy_queue->flag = 0x00;
        copy_queue->buffer = buffer;
        if (current_queue->odata != NULL) {
            copy_queue->odata = current_queue->odata;
            Py_INCREF(copy_queue->odata);
        } else {
            copy_queue->odata = NULL;
        }
        
        if (objbuf->buffer_head == NULL) {
            objbuf->buffer_head = copy_queue;
            objbuf->buffer_tail = copy_queue;
            objbuf->buffer_offset = buffer_offset;
        } else {
            if (buffer_offset > 0) {
                buffer = (PyBytesObject*)PyBytes_FromStringAndSize(0, buf_size);
                if (buffer == NULL) {
                    Py_DECREF(buffer);
                    BufferQueue_free(copy_queue);
                    return PyErr_NoMemory();
                }

                memcpy(buffer->ob_sval, copy_queue->buffer->ob_sval + buffer_offset, buf_size);
                Py_DECREF(copy_queue->buffer);
                copy_queue->buffer = buffer;
            }
            objbuf->buffer_tail->next = copy_queue;
            objbuf->buffer_tail = copy_queue;
        }
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + buf_size);
        current_queue->flag = 0x00;
        current_queue = current_queue->next;
        buffer_offset = 0;
        copy_size += buf_size;
        if (copy_size >= size) {
            return PyInt_FromLong((long) copy_size);
        }
    }

    return PyInt_FromLong((long) copy_size);
}

static PyObject *
Buffer_join(register BufferObject *objbuf) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    if(join_impl((BufferObject*) objbuf) != 0) {
        return PyErr_NoMemory();
    }

    PyObject* data = (PyObject*)objbuf->buffer_head->buffer;
    PyObject* odata = objbuf->buffer_head->odata;
    if(odata != NULL) {
        return PyTuple_Pack(2, data, odata);
    }
    Py_INCREF(data);
    return data;
}

static PyObject *
Buffer_clear(register BufferObject *objbuf) {
    if (Py_SIZE(objbuf) == 0) {
        Py_RETURN_NONE;
    }

    BufferQueue* last_queue;
    if(objbuf->buffer_offset > 0 && objbuf->buffer_head != NULL) {
        objbuf->buffer_offset = 0;
        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }

    while(objbuf->buffer_head != NULL) {
        last_queue = objbuf->buffer_head;
        objbuf->buffer_head = objbuf->buffer_head->next;
        PyBytesObject_free(last_queue->buffer, last_queue);
        BufferQueue_free(last_queue);
    }

    Py_SET_SIZE(objbuf, 0);
    objbuf->buffer_tail = NULL;
    Py_RETURN_NONE;
}

static PyObject*
Buffer_head(register BufferObject *objbuf) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    if(objbuf->buffer_head->odata != NULL) {
        return PyTuple_Pack(2, (PyObject*)objbuf->buffer_head->buffer, objbuf->buffer_head->odata);
    }
    Py_INCREF(objbuf->buffer_head->buffer);
    return (PyObject*)objbuf->buffer_head->buffer;
}

static PyObject*
Buffer_head_data(register BufferObject *objbuf) {
    if(Py_SIZE(objbuf) > 0 && objbuf->buffer_head->odata != NULL) {
        Py_INCREF(objbuf->buffer_head->odata);
        return (PyObject*)objbuf->buffer_head->odata;
    }
    Py_RETURN_NONE;
}

static PyObject*
Buffer_last(register BufferObject *objbuf) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    if(objbuf->buffer_tail->odata != NULL) {
        return PyTuple_Pack(2, (PyObject*)objbuf->buffer_tail->buffer, objbuf->buffer_tail->odata);
    }
    Py_INCREF(objbuf->buffer_tail->buffer);
    return (PyObject*)objbuf->buffer_tail->buffer;
}

static PyObject*
Buffer_last_data(register BufferObject *objbuf) {
    if(Py_SIZE(objbuf) > 0 && objbuf->buffer_tail->odata != NULL) {
        Py_INCREF(objbuf->buffer_tail->odata);
        return (PyObject*)objbuf->buffer_tail->odata;
    }
    Py_RETURN_NONE;
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
    int max_len = 0x7fffffff;
    if (!PyArg_ParseTuple(args, "i|i", &sock_fd, &max_len)) {
        return NULL;
    }
    max_len -= (int) Py_SIZE(objbuf);

    int max_count = socket_recv_count;
    PyBytesObject* buf;
    Py_ssize_t result = 0;
    Py_ssize_t recv_len = 0;

    while (max_count--) {
        if(objbuf->buffer_tail != NULL && objbuf->buffer_tail->flag == 0x01 && socket_recv_size - Py_SIZE(objbuf->buffer_tail->buffer) >= 256) {
            buf = objbuf->buffer_tail->buffer;
            result = recv(sock_fd, buf->ob_sval + Py_SIZE(buf), socket_recv_size - (int) Py_SIZE(buf), 0);
            if(result < 0) {
                if(CHECK_ERRNO(EWOULDBLOCK) || CHECK_ERRNO(EAGAIN)) {
                    return PyInt_FromLong((long) recv_len);
                }
                return set_error();
            }

            if(result == 0) {
                return PyInt_FromLong((long) recv_len);
            }

            Py_SET_SIZE(buf, Py_SIZE(buf) + result);
            Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + result);
            recv_len += result;
            if(recv_len > max_len) {
                return PyInt_FromLong((long) recv_len);
            }
            continue;
        }

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
                return PyInt_FromLong((long) recv_len);
            }
            return set_error();
        }

        if(result == 0) {
            if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
            } else {
                Py_DECREF(buf);
            }
            return PyInt_FromLong((long) recv_len);
        }

        Py_SET_SIZE(buf, result);

        BufferQueue* queue;
        if(buffer_queue_fast_buffer_index > 0) {
            queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
            queue->flag = 0x01;
        } else {
            queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
            if(queue == NULL) {
                if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                    bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
                } else {
                    Py_DECREF(buf);
                }
                return PyErr_NoMemory();
            }
            queue->flag = 0x01;
            queue->next = NULL;
            queue->odata = NULL;
        }
        queue->buffer = buf;

        if(objbuf->buffer_tail == NULL) {
            objbuf->buffer_head = queue;
            objbuf->buffer_tail = queue;
        } else {
            objbuf->buffer_tail->next = queue;
            objbuf->buffer_tail = queue;
        }
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + result);
        recv_len += result;
        if(recv_len > max_len) {
            return PyInt_FromLong((long) recv_len);
        }
    }
    return PyInt_FromLong((long) recv_len);
}

static PyObject *
Buffer_socket_send(register BufferObject *objbuf, PyObject *args)
{
    int sock_fd;
    if (!PyArg_ParseTuple(args, "i", &sock_fd)) {
        return NULL;
    }

    int max_count = socket_send_count;
    Py_ssize_t result = 0;
    Py_ssize_t send_len = 0;
    BufferQueue* last_queue;

    while (max_count-- && objbuf->buffer_head != NULL) {
        result = send(sock_fd, objbuf->buffer_head->buffer->ob_sval + objbuf->buffer_offset, Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset, 0);
        if(result < 0) {
            if(CHECK_ERRNO(EWOULDBLOCK) || CHECK_ERRNO(EAGAIN)) {
                return PyInt_FromLong((long) send_len);
            }
            return set_error();
        }

        if(result == 0) {
            return PyInt_FromLong((long) send_len);
        }

        objbuf->buffer_offset += result;
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - result);
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
        } else {
            return PyInt_FromLong((long) send_len);
        }
    }
    return PyInt_FromLong((long) send_len);
}

static PyObject *
Buffer_socket_recvfrom(register BufferObject *objbuf, PyObject *args)
{
    int sock_fd;
    int sa_family = AF_INET;
    int max_len = 0x7fffffff;
    if (!PyArg_ParseTuple(args, "i|ii", &sock_fd, &sa_family, &max_len)) {
        return NULL;
    }
    max_len -= (int) Py_SIZE(objbuf);

    int max_count = socket_recv_count;
    struct sockaddr_in6 addr;
    socklen_t addr_len = sizeof(struct sockaddr_in);
    if (sa_family == AF_INET6) {
        addr_len = sizeof(struct sockaddr_in6);
    }

    PyBytesObject* buf;
    PyObject* addr_data;
    Py_ssize_t result = 0;
    Py_ssize_t recv_len = 0;

    while (max_count--) {
        if(bytes_fast_buffer_index > 0) {
            buf = bytes_fast_buffer[--bytes_fast_buffer_index];
        } else {
            buf = (PyBytesObject*)PyBytes_FromStringAndSize(0, socket_recv_size);
            if(buf == NULL) {
                return PyErr_NoMemory();
            }
        }

        memset(&addr, 0, sizeof(struct sockaddr_in6));
        result = recvfrom(sock_fd, buf->ob_sval, socket_recv_size, 0, (struct sockaddr*)&addr, &addr_len);
        if(result < 0) {
            if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
            } else {
                Py_DECREF(buf);
            }
            if(CHECK_ERRNO(EWOULDBLOCK) || CHECK_ERRNO(EAGAIN)) {
                return PyInt_FromLong((long) recv_len);
            }
            return set_error();
        }

        if(result == 0) {
            if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
            } else {
                Py_DECREF(buf);
            }
            return PyInt_FromLong((long) recv_len);
        }

        Py_SET_SIZE(buf, result);
        if (inet_ntop(sa_family, sa_family == AF_INET ? (void*)&((struct sockaddr_in*)&addr)->sin_addr : (void*)&((struct sockaddr_in6*)&addr)->sin6_addr,
                               sock_addr_host, 64) == NULL) {
            if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
            } else {
                Py_DECREF(buf);
            }
            PyErr_SetString(PyExc_OSError, "host inet_ntop error");
            return NULL;
        }
        if (sa_family == AF_INET) {
            addr_data = PyTuple_Pack(2, PyString_FromString(sock_addr_host), PyInt_FromLong(ntohs(((struct sockaddr_in*)&addr)->sin_port)));
        } else {
            addr_data = PyTuple_Pack(4, PyString_FromString(sock_addr_host), PyInt_FromLong(ntohs(((struct sockaddr_in6*)&addr)->sin6_port)),
                                     PyInt_FromLong(ntohl(((struct sockaddr_in6*)&addr)->sin6_flowinfo)), PyInt_FromLong(((struct sockaddr_in6*)&addr)->sin6_scope_id));
        }

        BufferQueue* queue;
        if(buffer_queue_fast_buffer_index > 0) {
            queue = buffer_queue_fast_buffer[--buffer_queue_fast_buffer_index];
            queue->flag = 0x01;
        } else {
            queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
            if(queue == NULL) {
                if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
                    bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
                } else {
                    Py_DECREF(buf);
                }
                Py_DECREF(addr_data);
                return PyErr_NoMemory();
            }
            queue->flag = 0x01;
            queue->next = NULL;
        }
        queue->odata = addr_data;
        queue->buffer = buf;

        if(objbuf->buffer_tail == NULL) {
            objbuf->buffer_head = queue;
            objbuf->buffer_tail = queue;
        } else {
            objbuf->buffer_tail->next = queue;
            objbuf->buffer_tail = queue;
        }
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) + result);
        recv_len += result;
        if(recv_len > max_len) {
            return PyInt_FromLong((long) recv_len);
        }
    }
    return PyInt_FromLong((long) recv_len);
}

static PyObject *
Buffer_socket_sendto(register BufferObject *objbuf, PyObject *args)
{
    int sock_fd;
    int sa_family = AF_INET;
    if (!PyArg_ParseTuple(args, "i|i", &sock_fd, &sa_family)) {
        return NULL;
    }

    int max_count = socket_send_count;
    struct sockaddr_in6 addr;
    socklen_t addr_len = sizeof(struct sockaddr_in);
    if (sa_family == AF_INET6) {
        addr_len = sizeof(struct sockaddr_in6);
    }
    memset(&addr, 0, sizeof(struct sockaddr_in6));
    ((struct sockaddr_in6*)&addr)->sin6_family = sa_family;

    Py_ssize_t result = 0;
    Py_ssize_t send_len = 0;
    BufferQueue* last_queue;

    PyObject* host;
    PyObject* port;
    PyObject* flowinfo;
    PyObject* scope_id;

    while (max_count-- && objbuf->buffer_head != NULL) {
        if (objbuf->buffer_head->odata == NULL || !PyTuple_CheckExact(objbuf->buffer_head->odata)) {
            PyErr_SetString(PyExc_OSError, "buffer data must be sock address");
            return NULL;
        }

        if(PyTuple_GET_SIZE(objbuf->buffer_head->odata) < 2) {
            PyErr_SetString(PyExc_OSError, "sock address must be has host and port");
            return NULL;
        }

        host = PyTuple_GET_ITEM(objbuf->buffer_head->odata, 0);
        if (!PyString_CheckExact(host)) {
            if(!PyStringB_CheckExact(host)) {
                PyErr_SetString(PyExc_OSError, "sock host must be string");
                return NULL;
            } else {
#if PY_MAJOR_VERSION >= 3
                const char *host_chars = PyBytes_AS_STRING(host);
#else
                PyObject* host_string_obj = PyUnicode_AsUTF8String(host);
                const char *host_chars = PyString_AS_STRING(host_string_obj);
#endif
                if(inet_pton(sa_family, host_chars, sa_family == AF_INET ? (void *)&((struct sockaddr_in*)&addr)->sin_addr : (void *)&((struct sockaddr_in6*)&addr)->sin6_addr) != 1) {
#if PY_MAJOR_VERSION < 3
                    Py_DECREF(host_string_obj);
#endif
                    PyErr_SetString(PyExc_OSError, "host inet_pton error");
                    return NULL;
                }
#if PY_MAJOR_VERSION < 3
                Py_DECREF(host_string_obj);
#endif
            }
        } else {
#if PY_MAJOR_VERSION >= 3
            const char *host_chars = PyUnicode_AsUTF8(host);
#else
            const char *host_chars = PyString_AS_STRING(host);
#endif
            if(inet_pton(sa_family, host_chars, sa_family == AF_INET ? (void *)&((struct sockaddr_in*)&addr)->sin_addr : (void *)&((struct sockaddr_in6*)&addr)->sin6_addr) != 1) {
                PyErr_SetString(PyExc_OSError, "host inet_pton error");
                return NULL;
            }
        }

        port = PyTuple_GET_ITEM(objbuf->buffer_head->odata, 1);
        if (!PyInt_CheckExact(port)) {
            PyErr_SetString(PyExc_OSError, "sock host must be number");
            return NULL;
        }
        if (PyInt_AS_LONG(port) < 0 || PyInt_AS_LONG(port) > 0xffff) {
            PyErr_SetString(PyExc_OverflowError, "sock port must be 0-65535.");
            return NULL;
        }
        ((struct sockaddr_in*)&addr)->sin_port = htons((short)PyInt_AS_LONG(port));

        if (sa_family == AF_INET6) {
            if (PyTuple_GET_SIZE(objbuf->buffer_head->odata) >= 3) {
                flowinfo = PyTuple_GET_ITEM(objbuf->buffer_head->odata, 2);
                if (!PyInt_CheckExact(flowinfo)) {
                    PyErr_SetString(PyExc_OSError, "sock flowinfo must be number");
                    return NULL;
                }
                if (PyInt_AS_LONG(flowinfo) > 0xfffff) {
                    PyErr_SetString(PyExc_OverflowError, "flowinfo must be 0-1048575.");
                    return NULL;
                }
                ((struct sockaddr_in6*)&addr)->sin6_flowinfo = htonl((unsigned int)PyInt_AS_LONG(flowinfo));
            } else {
                ((struct sockaddr_in6*)&addr)->sin6_flowinfo = htonl(0);
            }

            if (PyTuple_GET_SIZE(objbuf->buffer_head->odata) >= 4) {
                scope_id = PyTuple_GET_ITEM(objbuf->buffer_head->odata, 3);
                if (!PyInt_CheckExact(scope_id)) {
                    PyErr_SetString(PyExc_OSError, "sock flowinfo must be number");
                    return NULL;
                }
                ((struct sockaddr_in6*)&addr)->sin6_flowinfo = (unsigned int)PyInt_AS_LONG(scope_id);
            } else {
                ((struct sockaddr_in6*)&addr)->sin6_flowinfo = 0;
            }
        }

        result = sendto(sock_fd, objbuf->buffer_head->buffer->ob_sval + objbuf->buffer_offset, Py_SIZE(objbuf->buffer_head->buffer) - objbuf->buffer_offset, 0, (struct sockaddr*)&addr, addr_len);
        if(result < 0) {
            if(CHECK_ERRNO(EWOULDBLOCK) || CHECK_ERRNO(EAGAIN)) {
                return PyInt_FromLong((long) send_len);
            }
            return set_error();
        }

        if(result == 0) {
            return PyInt_FromLong((long) send_len);
        }

        objbuf->buffer_offset += result;
        Py_SET_SIZE(objbuf, Py_SIZE(objbuf) - result);
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
        } else {
            return PyInt_FromLong((long) send_len);
        }
    }
    return PyInt_FromLong((long) send_len);
}

static PyObject*
Buffer_buffer_getter(register BufferObject *objbuf, void *args) {
    if(Py_SIZE(objbuf) == 0) {
        return PyBytes_FromStringAndSize(0, 0);
    }

    if(objbuf->buffer_head->odata != NULL) {
        return PyTuple_Pack(2, (PyObject*)objbuf->buffer_head->buffer, objbuf->buffer_head->odata);
    }
    Py_INCREF(objbuf->buffer_head->buffer);
    return (PyObject*)objbuf->buffer_head->buffer;
}

static PyObject*
Buffer_buffer_len_getter(register BufferObject *objbuf, void *args) {
    if(Py_SIZE(objbuf) == 0) {
        return PyInt_FromLong(0);
    }

    return PyInt_FromLong((long) Py_SIZE(objbuf->buffer_head->buffer));
}

static PyObject*
Buffer_buffers_getter(register BufferObject *objbuf, void *args) {
    PyObject* buffers = PyList_New(0);

    if(Py_SIZE(objbuf) == 0) {
        return buffers;
    }

    BufferQueue *current = objbuf->buffer_head;
    while (current != NULL) {
        if(current->odata != NULL) {
            PyList_Append(buffers, PyTuple_Pack(2, current->buffer, current->odata));
        } else {
            PyList_Append(buffers, (PyObject*)current->buffer);
        }
        current = current->next;
    }

    return buffers;
}

static PyMemberDef Buffer_members[] = {
        {"_buffer_index", T_INT, offsetof(BufferObject, buffer_offset), READONLY, "buffer_offset"},
#if PY_MAJOR_VERSION >= 3
        {"_len", T_INT, offsetof(PyVarObject, ob_size), READONLY, "buffer_len"},
#else
        {"_len", T_INT, offsetof(BufferObject, ob_size), READONLY, "buffer_len"},
#endif
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
        {"next", (PyCFunction)Buffer_next, METH_NOARGS, "buffer next"},
        {"extend", (PyCFunction)Buffer_extend, METH_O, "buffer extend"},
        {"fetch", (PyCFunction)Buffer_fetch, METH_VARARGS, "buffer fetch"},
        {"copyfrom", (PyCFunction)Buffer_copyfrom, METH_VARARGS, "buffer copyfrom"},
        {"join", (PyCFunction)Buffer_join, METH_NOARGS, "buffer join"},
        {"clear", (PyCFunction)Buffer_clear, METH_NOARGS, "buffer clear"},
        {"head", (PyCFunction)Buffer_head, METH_NOARGS, "buffer head"},
        {"head_data", (PyCFunction)Buffer_head_data, METH_NOARGS, "buffer head_data"},
        {"last", (PyCFunction)Buffer_last, METH_NOARGS, "buffer last"},
        {"last_data", (PyCFunction)Buffer_last_data, METH_NOARGS, "buffer last_data"},
        {"socket_send", (PyCFunction)Buffer_socket_send, METH_VARARGS, "buffer socket_send"},
        {"socket_recv", (PyCFunction)Buffer_socket_recv, METH_VARARGS, "buffer socket_recv"},
        {"socket_sendto", (PyCFunction)Buffer_socket_sendto, METH_VARARGS, "buffer socket_sendto"},
        {"socket_recvfrom", (PyCFunction)Buffer_socket_recvfrom, METH_VARARGS, "buffer socket_recvfrom"},
        {NULL}  /* Sentinel */
};

static PySequenceMethods Buffer_as_sequence = {
        (lenfunc)Buffer_length,             /*sq_length*/
        0,                                  /*sq_concat*/
        0,                                  /*sq_repeat*/
        (ssizeargfunc)Buffer_item,          /*sq_item*/
        (ssizessizeargfunc)Buffer_slice,    /*sq_slice*/
        0,                                  /*sq_ass_item*/
        0,                                  /*sq_ass_slice*/
        0                                   /*sq_contains*/
};

static PyNumberMethods Buffer_as_number = {
        0, //nb_add;
        0, //nb_subtract;
        0, //nb_multiply;
#if PY_MAJOR_VERSION < 3
        0, //nb_divide;
#endif
        0, //nb_remainder;
        0, //nb_divmod;
        0, //nb_power;
        0, //nb_negative;
        0, //nb_positive;
        0, //nb_absolute;
        (inquiry)Buffer_nonzero, //nb_nonzero;
};



static PyBufferProcs Buffer_as_buffer = {
#if PY_MAJOR_VERSION < 3
        (readbufferproc)Buffer_getreadbuf,      /*bf_getreadbuffer*/
        (writebufferproc)Buffer_getwritebuf,    /*bf_getwritebuffer*/
        (segcountproc)Buffer_getsegcount,       /*bf_getsegcount*/
        (charbufferproc)Buffer_getcharbuf,      /*bf_getcharbuffer*/
#endif
        (getbufferproc)Buffer_getbuffer,        /*bf_getbuffer*/
        0,                                      /*bf_releasebuffer*/
};

static PyTypeObject BufferType = {
#ifndef MS_WINDOWS
        PyVarObject_HEAD_INIT(&PyType_Type, 0)    /*ob_size*/
#else
        PyVarObject_HEAD_INIT(NULL, 0)            /*ob_size*/
#endif
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
        &Buffer_as_buffer,                        /*tp_as_buffer*/
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
#ifndef MS_WINDOWS
        &PyBaseObject_Type,                       /* tp_base */
#else
        NULL,                                     /* tp_base */
#endif
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

    Py_ssize_t result = recv(sock_fd, buf->ob_sval, socket_recv_size, 0);
    if(result < 0) {
        if(bytes_fast_buffer_index < BYTES_FAST_BUFFER_COUNT) {
            bytes_fast_buffer[bytes_fast_buffer_index++]=buf;
        } else {
            Py_DECREF(buf);
        }
        return set_error();
    }

    Py_SET_SIZE(buf, result);
    return (PyObject *)buf;
}

static PyObject *
cbuffer_socket_send(PyObject *self, PyObject *args) {
    int sock_fd;
    PyObject* data;
    if (!PyArg_ParseTuple(args, "iO", &sock_fd, &data)) {
        return NULL;
    }

    if(!PyBytes_CheckExact(data)) {
        PyErr_SetString(PyExc_TypeError, "The data must be a bytes");
        return NULL;
    }

    Py_ssize_t result = send(sock_fd, ((PyBytesObject*)data)->ob_sval, (int) Py_SIZE(data), 0);
    if(result < 0) {
        return set_error();
    }

    return PyInt_FromLong((long) result);
}

static PyObject *
cbuffer_socket_set_recv_size(PyObject *self, PyObject *args) {
    int recv_size;
    if (!PyArg_ParseTuple(args, "i", &recv_size)) {
        return NULL;
    }

    if(bytes_fast_buffer_index > 0) {
        PyErr_SetString(PyExc_RuntimeError, "The fast bytes is inited");
        return NULL;
    }

    socket_recv_size = recv_size;
    Py_RETURN_NONE;
}

static PyObject *
cbuffer_socket_get_recv_size(PyObject *self, PyObject *args) {
    return PyInt_FromLong(socket_recv_size);
}

static PyObject *
cbuffer_socket_set_recv_count(PyObject *self, PyObject *args) {
    int recv_count;
    if (!PyArg_ParseTuple(args, "i", &recv_count)) {
        return NULL;
    }

    socket_recv_count = recv_count;
    Py_RETURN_NONE;
}

static PyObject *
cbuffer_socket_get_recv_count(PyObject *self, PyObject *args) {
    return PyInt_FromLong(socket_recv_count);
}

static PyObject *
cbuffer_socket_set_send_count(PyObject *self, PyObject *args) {
    int send_count;
    if (!PyArg_ParseTuple(args, "i", &send_count)) {
        return NULL;
    }

    socket_send_count = send_count;
    Py_RETURN_NONE;
}

static PyObject *
cbuffer_socket_get_send_count(PyObject *self, PyObject *args) {
    return PyInt_FromLong(socket_send_count);
}

static PyMethodDef module_methods[] =
{
        {"socket_send", (PyCFunction)cbuffer_socket_send, METH_VARARGS, "socket_send"},
        {"socket_recv", (PyCFunction)cbuffer_socket_recv, METH_VARARGS, "socket_recv"},
        {"socket_set_recv_size", (PyCFunction)cbuffer_socket_set_recv_size, METH_VARARGS, "socket_set_recv_size"},
        {"socket_get_recv_size", (PyCFunction)cbuffer_socket_get_recv_size, METH_VARARGS, "socket_get_recv_size"},
        {"socket_set_recv_count", (PyCFunction)cbuffer_socket_set_recv_count, METH_VARARGS, "socket_set_recv_count"},
        {"socket_get_recv_count", (PyCFunction)cbuffer_socket_get_recv_count, METH_VARARGS, "socket_get_recv_count"},
        {"socket_set_send_count", (PyCFunction)cbuffer_socket_set_send_count, METH_VARARGS, "socket_set_send_count"},
        {"socket_get_send_count", (PyCFunction)cbuffer_socket_get_send_count, METH_VARARGS, "socket_get_send_count"},
        {NULL, NULL, 0, NULL}
};

int cbuffer_init(PyObject* m) {
    Py_INCREF((PyObject *)&BufferType);
    if (PyModule_AddObject(m, "Buffer", (PyObject *)&BufferType) != 0)
        return -1;

    int init_buffer_queue_fast_buffer_count = BUFFER_QUEUE_FAST_BUFFER_COUNT / 3;
    while (buffer_queue_fast_buffer_index < init_buffer_queue_fast_buffer_count) {
        BufferQueue* buffer_queue = (BufferQueue*)PyMem_Malloc(sizeof(BufferQueue));
        if(buffer_queue == NULL) {
            return 0;
        }

        buffer_queue->next = NULL;
        buffer_queue->flag = 0;
        buffer_queue->buffer = NULL;
        buffer_queue->odata = NULL;
        buffer_queue_fast_buffer[buffer_queue_fast_buffer_index] = buffer_queue;
        buffer_queue_fast_buffer_index++;
    }
    return 0;
}

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef cbuffermodule = {
        PyModuleDef_HEAD_INIT,
        "cbuffer",
        "cbuffer",
        -1,
        module_methods,
        NULL,
        NULL,
        NULL,
        NULL
};

PyMODINIT_FUNC
PyInit_cbuffer() {
    PyObject *m;
    if (PyType_Ready(&BufferType) < 0) {
        return NULL;
    }

    m = PyModule_Create(&cbuffermodule);
    if (m == NULL)
        return NULL;

    if (cbuffer_init(m) != 0) {
        return NULL;
    }

    return m;
}
#else
PyMODINIT_FUNC
initcbuffer() {
    PyObject *m;
    if (PyType_Ready(&BufferType) < 0) {
        return;
    }

    m = Py_InitModule3("cbuffer", module_methods, "cbuffer");
    if (m == NULL) {
        return;
    }

    if (cbuffer_init(m) != 0) {
        return;
    }
}
#endif