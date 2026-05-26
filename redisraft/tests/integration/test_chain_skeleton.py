"""
Phase A skeleton verification for chain replication RPCs in cluster_rdma_chain.c.

Tests only the control-plane / parse / reply layer. No actual RDMA QPs or
buffer registration are exercised yet (Phase A.full). End-to-end RDMA-WRITE
verification through the chain happens via RDMA CHAIN-PING and requires
hardware — out of scope for this test.
"""

import pytest


def test_chain_prep_returns_real_addr_placeholder_rkey(cluster):
    """RDMA CHAIN-PREP replies with a 4-element array: [status, addr, rkey, bytes].
    After Phase A.full's mmap step: addr is the real mmap'd pool VA (non-zero,
    page-aligned); rkey is still 0 (ibv_reg_mr deferred until RDMA hardware
    integration); bytes echoes the request."""
    cluster.create(1)
    reply = cluster.execute('RDMA', 'CHAIN-PREP', 42, 4096)
    assert isinstance(reply, list) and len(reply) == 4
    assert reply[0] == b'CHAIN-PREP-OK'
    assert reply[1] > 0            # addr = mmap'd pool VA
    assert reply[1] % 4096 == 0    # page-aligned (mmap guarantees this)
    assert reply[2] == 0           # rkey (still placeholder)
    assert reply[3] == 4096        # bytes echoed back


def test_chain_prep_rejects_bad_pool_bytes(cluster):
    cluster.create(1)
    with pytest.raises(Exception, match='pool_bytes must be positive'):
        cluster.execute('RDMA', 'CHAIN-PREP', 42, 0)


def test_chain_prep_idempotent_for_same_session(cluster):
    """Calling CHAIN-PREP twice with the same (session, pool_bytes) is OK
    (idempotent) — the follower just keeps its existing state."""
    cluster.create(1)
    r1 = cluster.execute('RDMA', 'CHAIN-PREP', 42, 4096)
    r2 = cluster.execute('RDMA', 'CHAIN-PREP', 42, 4096)
    assert r1 == r2


def test_chain_prep_rejects_size_change(cluster):
    """If a session is already PREP'd at one size, a second PREP with a
    different size must error out."""
    cluster.create(1)
    cluster.execute('RDMA', 'CHAIN-PREP', 42, 4096)
    with pytest.raises(Exception, match='mismatched pool_bytes'):
        cluster.execute('RDMA', 'CHAIN-PREP', 42, 8192)


def test_chain_wire_requires_prior_prep(cluster):
    """CHAIN-WIRE without a prior CHAIN-PREP for the same session must error."""
    cluster.create(1)
    with pytest.raises(Exception, match='no CHAIN-PREP state'):
        cluster.execute(
            'RDMA', 'CHAIN-WIRE',
            999, 1, 2,
            '-', 0,             # predecessor (no real pred for a 2-link chain head)
            '-', 0, 0, 0, 0,    # successor (tail) + succ_rdma_port=0
        )


def test_chain_wire_succeeds_after_prep(cluster):
    """Happy path: PREP first, then WIRE replies +OK and stores chain
    position / succ info."""
    cluster.create(1)
    cluster.execute('RDMA', 'CHAIN-PREP', 77, 2048)
    reply = cluster.execute(
        'RDMA', 'CHAIN-WIRE',
        77, 2, 2,           # I'm position 2 of 2 → I'm the tail
        '127.0.0.1', 6379,
        '-', 0, 0, 0, 0,    # tail: no successor + succ_rdma_port=0
    )
    assert reply == b'OK'


def test_chain_ping_requires_prior_prep(cluster):
    cluster.create(1)
    with pytest.raises(Exception, match='no chain state'):
        cluster.execute('RDMA', 'CHAIN-PING', 12345, 0)


def test_chain_ping_succeeds_after_prep(cluster):
    cluster.create(1)
    cluster.execute('RDMA', 'CHAIN-PREP', 12345, 4096)
    reply = cluster.execute('RDMA', 'CHAIN-PING', 12345, 0)
    assert reply == b'OK'


def test_debug_chain_establish_multi_node(cluster):
    """End-to-end orchestrator: bring up a 3-node cluster, drive
    DEBUG-CHAIN-ESTABLISH on the leader with the other two as followers,
    verify both followers have chain state via CHAIN-PING."""
    cluster.create(3)
    assert cluster.leader == 1

    f1 = cluster.node(2)
    f2 = cluster.node(3)
    f1_host = f1.address.split(':')[0]
    f1_port = int(f1.address.split(':')[1])
    f2_host = f2.address.split(':')[0]
    f2_port = int(f2.address.split(':')[1])

    # Leader orchestrates: PREP both followers, then WIRE both.
    reply = cluster.execute(
        'RDMA', 'DEBUG-CHAIN-ESTABLISH', 7777, 4096,
        f1_host, f1_port, f2_host, f2_port)
    assert reply == b'OK'

    # Each follower should now have chain state for sess=7777; CHAIN-PING
    # succeeds when state exists (skeleton-level verification — real byte
    # comparison comes in Phase A.full with RDMA hardware).
    assert f1.client.execute_command('RDMA', 'CHAIN-PING', 7777, 0) == b'OK'
    assert f2.client.execute_command('RDMA', 'CHAIN-PING', 7777, 0) == b'OK'


def test_debug_chain_establish_rejects_unreachable_follower(cluster):
    """If even one follower is unreachable, orchestrator must abort."""
    cluster.create(1)
    # Use a port that's almost certainly not listening
    with pytest.raises(Exception, match='DEBUG-CHAIN-ESTABLISH'):
        cluster.execute(
            'RDMA', 'DEBUG-CHAIN-ESTABLISH', 8888, 4096,
            '127.0.0.1', 1)
