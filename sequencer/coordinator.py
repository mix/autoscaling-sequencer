import logging

from kazoo.client import KazooClient, KazooState
from constants import zk_sequencer_root
from strategy import SequenceStrategy


class DistributedSequenceCoordinator(object):
    def __init__(self, zookeeper_connect, autoscaling_grp_name, strategy_name, instance_id, max_sequence_id,
                 asg_instances_ids):
        self.zk = KazooClient(hosts=zookeeper_connect)
        self.running = False
        self.interrupted = False
        self.autoscaling_grp_name = autoscaling_grp_name
        self.strategy_name = strategy_name
        self.instance_id = instance_id
        self.max_sequence_id = max_sequence_id
        self.asg_instances_ids = asg_instances_ids

    def state_change_listener(self, state):
        logging.debug('zookeeper state changed to {0}'.format(state))
        if state == KazooState.LOST or state == KazooState.SUSPENDED:
            if self.running:
                self.interrupted = True
                self.log_msg('distributed coordination interrupted')
                raise Exception('zookeeper session interrupted')

    """
     Responsible for executing operation in isolation even-in cases of failures, connection-resets etc. Uses optimistic
     concurrency control by assuming that operation would be executed without any interruption, and if any interruption
     occurs, then acquires a new lock and re-execute the idempotent operation to guarantee isolation.
    """

    def execute(self):
        result = None

        # exception-handling for cases where unable to establish connection to zookeeper
        try:
            # TODO: use python retrying lib to control with timeouts, max & exponential back-off wait time b/w retries
            while result is None or self.interrupted:
                self.running = True
                self.interrupted = False
                self.log_msg('distributed operation starting')
                self.zk.start()
                self.zk.add_listener(self.state_change_listener)
                try:
                    lock = self.zk.Lock(zk_sequencer_root, self.autoscaling_grp_name)
                    logging.debug('zookeeper lock created {}'.format(lock.data))
                    self.log_msg('entering zookeeper lock')
                    with lock:
                        result = self.operation()
                except Exception as e:
                    logging.exception(e)
                    self.log_msg('encountered zk exception')
                finally:
                    self.log_msg('stopping zk')
                    self.zk.stop()
        except Exception as e:
            raise e

        if result is None:
            raise Exception('Unable to generate sequence id')

        return result

    def operation(self):
        instances_root_path = "/".join([zk_sequencer_root, self.autoscaling_grp_name])
        self.zk.ensure_path(instances_root_path)
        instance_nodes = self.zk.get_children(instances_root_path)
        zk_instance_sequencers = {}
        for instance_node in instance_nodes:
            instance_node_path = "/".join([instances_root_path, instance_node])
            instance_id = self.zk.get(instance_node_path)[0]
            zk_instance_sequencers[str(instance_id)] = int(instance_node)

        logging.debug('zk instances: {0}'.format(zk_instance_sequencers))

        instance_sequencers = {k: v for k, v in zk_instance_sequencers.items() if k in self.asg_instances_ids}

        logging.debug('active instances with assigned sequences: {0}'.format(instance_sequencers))

        generator = SequenceStrategy(self.strategy_name,
                                     self.instance_id,
                                     instance_sequencers,
                                     self.max_sequence_id)

        sequence_id = generator.get_sequence_id()
        current_instance_node_path = "/".join([instances_root_path, str(sequence_id)])
        self.zk.ensure_path(current_instance_node_path)
        self.zk.set(current_instance_node_path, str.encode(str(self.instance_id)))
        self.running = False
        return sequence_id

    def log_msg(self, msg):
        logging.debug('{0}, running = {1}, interrupted = {2}'.format(msg, self.running, self.interrupted))