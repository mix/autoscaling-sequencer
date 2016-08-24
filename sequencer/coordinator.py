import logging

from kazoo.exceptions import NodeExistsError
from kazoo.client import KazooClient, KazooState

from constants import zk_sequencer_root
from strategy import SequenceStrategy


class DistributedSequenceCoordinator(object):
    def __init__(self, zookeeper_connect, autoscaling_grp_name, strategy_name, instance_id, max_sequence_id,
                 asg_instances_ids):
        self.zk = KazooClient(hosts=zookeeper_connect)
        self.interrupted = False
        self.autoscaling_grp_name = autoscaling_grp_name
        self.strategy_name = strategy_name
        self.instance_id = instance_id
        self.max_sequence_id = max_sequence_id
        self.asg_instances_ids = asg_instances_ids

    def state_change_listener(self, state):
        logging.debug('zookeeper state changed to {0}'.format(state))
        if state == KazooState.LOST or state == KazooState.SUSPENDED:
            logging.debug('distributed sequence coordination interrupted')
            self.interrupted = True

    def execute(self):
        self.zk.start()
        self.zk.add_listener(self.state_change_listener)
        result = None
        try:
            while result is None:
                lock = self.zk.Lock(zk_sequencer_root, self.autoscaling_grp_name)
                logging.debug('zookeeper lock created {}'.format(lock.data))
                with lock:
                    logging.debug('distributed sequence coordination entered zookeeper lock')
                    instances_root_path = "/".join([zk_sequencer_root, self.autoscaling_grp_name])
                    self.zk.ensure_path(instances_root_path)
                    instance_nodes = self.zk.get_children(instances_root_path)
                    zk_instance_sequencers = {}
                    for instance_node in instance_nodes:
                        instance_node_path = "/".join([instances_root_path, instance_node])
                        instance_id = self.zk.get(instance_node_path)[0]
                        zk_instance_sequencers[str(instance_id)] = int(instance_node)

                    instance_sequencers = {k: v for k, v in zk_instance_sequencers.items() if
                                           k in self.asg_instances_ids}
                    print('zookeeper_state = {0}'.format(zk_instance_sequencers))
                    print('autoscaling_grp = {0}'.format(self.asg_instances_ids))
                    print('active_state = {0}'.format(instance_sequencers))

                    generator = SequenceStrategy(self.strategy_name,
                                                 self.instance_id,
                                                 instance_sequencers,
                                                 self.max_sequence_id)

                    sequence_id = generator.get_sequence_id()
                    current_instance_node_path = "/".join([instances_root_path, str(sequence_id)])
                    if not self.interrupted:
                        try:
                            self.zk.create(path=current_instance_node_path,
                                           value=str.encode(str(self.instance_id)),
                                           ephemeral=False,
                                           makepath=True)
                        except NodeExistsError:
                            self.zk.delete(current_instance_node_path)
                            self.zk.create(path=current_instance_node_path,
                                           value=str.encode(str(self.instance_id)),
                                           ephemeral=False,
                                           makepath=True)

                # optimistic concurrency control by assuming that operation would be executed without any
                # interruption but if any interruption happens, then idempotent operation is executed again
                if self.interrupted:
                    logging.debug('distributed sequence coordination was interrupted so retrying with lock')
                    result = None
                    continue
                else:
                    result = sequence_id
        finally:
            self.zk.stop()

        return result