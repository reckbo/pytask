import task


def hash_update(M, elems):
    '''
    M = hash_update(M, elems)
    Update the hash object ``M`` with the sequence ``elems``.
    Parameters
    ----------
    M : hashlib object
        An object on which the update method will be called
    elems : sequence of 2-tuples
    Returns
    -------
    M : hashlib object
        This is the same object as the argument
    '''
    from six.moves import cPickle as pickle
    from six.moves import map
    import six

    try:
        import numpy as np
    except ImportError:
        np = None
    for n, e in elems:
        M.update(pickle.dumps(n))
        if isinstance(e, task.Task):
            M.update(e.hash())
        elif type(e) in (list, tuple):
            M.update(repr(type(e)).encode('utf-8'))
            hash_update(M, enumerate(e))
        elif type(e) == set:
            M.update(six.b('set'))
            # With randomized hashing, different runs of Python might result in
            # different orders, so sort. We cannot trust that all the elements
            # in the set will be comparable, so we convert them to their hashes
            # beforehand.
            items = list(map(hash_one, e))
            items.sort()
            hash_update(M, enumerate(items))
        elif type(e) == dict:
            M.update(six.b('dict'))
            items = [(hash_one(k), v) for k, v in e.items()]
            items.sort(key=(lambda k_v: k_v[0]))

            hash_update(M, items)
        elif np is not None and type(e) == np.ndarray:
            M.update(six.b('np.ndarray'))
            M.update(pickle.dumps(e.dtype))
            M.update(pickle.dumps(e.shape))
            try:
                buffer = e.data
                M.update(buffer)
            except:
                M.update(e.copy().data)
        else:
            M.update(pickle.dumps(e))
    return M


def hash_one(obj):
    '''
    hvalue = hash_one(obj)
    Compute a hash from a single object
    Parameters
    ----------
    obj : object
        Hashable object
    Returns
    -------
    hvalue : str
    '''
    import hashlib
    h = hashlib.sha1()
    hash_update(h, [('hash1', obj)])
    return h.hexdigest().encode('utf-8')
