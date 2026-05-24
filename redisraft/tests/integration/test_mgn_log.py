"""
Verification for the RAFT.MGN-LOG protocol-event command.

Under option (b) the migration is driven by the existing migrationWorker in
cluster_rdma.c; this command is the means by which that worker records phase
boundaries in the Raft log so all replicas (donor + recipient sides) see the
same view of in-flight sessions.

This test exercises the wiring without involving cluster_rdma.c: it issues
each of the five RAFT.MGN-LOG <type> <payload> calls directly and asserts
that the corresponding entry replicates and applies on every node.
"""

import pytest

TYPES = [
    ('TXN_START',      'sess=1 slots=0-100 recipient=dbid-r'),
    ('RECP_TXN_START', 'sess=1 slots=0-100 donor=dbid-d'),
    ('INDX_UPD',       'sess=1 idx=99'),
    ('RECP_TXN_DONE',  'sess=1'),
    ('TXN_DONE',       'sess=1'),
]


@pytest.mark.parametrize('type_name,payload', TYPES)
def test_raft_mgn_log_replicates(cluster, type_name, payload):
    cluster.create(3)
    assert cluster.leader == 1

    reply = cluster.execute('raft.mgn-log', type_name, payload)
    assert reply == b'OK'

    target_idx = cluster.leader_node().info()['raft_current_index']
    for node_id in (1, 2, 3):
        cluster.node(node_id).wait_for_commit_index(target_idx, gt_ok=True)
        cluster.node(node_id).wait_for_log_applied()


def test_raft_mgn_log_rejects_unknown_type(cluster):
    cluster.create(1)
    with pytest.raises(Exception, match='unknown MGN log type'):
        cluster.execute('raft.mgn-log', 'NOPE', 'payload')
