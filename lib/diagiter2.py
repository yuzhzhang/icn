#!/usr/bin/env python
# -*- coding=utf-8 -*-

"""
Author: Yuzhong Zhang (arro.zhyzh@gmail.com)
Created: Jan 25, 2014
Last Update: Sep 08, 2014
"""

import numpy as np
import numpy.linalg as npl
import time

def modify_super(D, W, f, lambd, c2, v):
    n = len(f)
    D += np.diag(c2) * 2.0 / lambd * np.eye(n)
    f += 2.0 * c2 * v
    return D, f

def diagiter(D, W, f, lambd, c1, c2, v, lb=None, ub=None, delta=None):
    tic = time.time()

    thold = 1e-5
    max_iter = 300
    max_runtime = 0.25

    n = len(f)

    if lb is None:
        lb = -np.inf * np.ones(np.shape(f))
    if ub is None:
        ub = np.inf * np.ones(np.shape(f))
    if delta is None:
        if npl.norm(npl.solve(D, W)) >= 1:
            delta = npl.norm(W)
        else:
            delta = 0.0

    if np.any(np.abs(c2) > 1e-10):
        D, f = modify_super(D, W, f, lambd, c2, v)

    u = v
    Dhat = D + delta * np.eye(n)
    What = W - delta * np.eye(n)

    dDhat = lambd * np.diag(Dhat)[:, None]
    tmp1 = c1 / dDhat
    What2 = lambd * What
    tmpf = f / dDhat
    
    exit_flag = 1
    for i in range(1, max_iter):
        toc = time.time()
        if toc - tic > max_runtime:
            break

        u_ = u

        tmp2 = tmpf - What2.dot(u) / dDhat
        u_b = tmp2 - tmp1
        u_s = tmp2 + tmp1

        u_b = np.minimum(np.maximum(np.maximum(u_b, v), lb), ub)
        u_s = np.minimum(np.maximum(np.minimum(u_s, v), lb), ub)

        u = v + np.maximum(u_b - v, 0) + np.minimum(u_s -v, 0)

        if npl.norm(u - u_) < thold:
            #print "thold=", thold
            #print "Norm=", npl.norm(u - u_)
            #print "Iter = ", i
            exit_flag = 0
            break
    #print 'iter =',i

    return u, exit_flag
