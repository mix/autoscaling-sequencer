import logging


class SequenceStrategy(object):
    def __init__(self, strategy_name, instance_id, instance_sequences, max_sequence_id):

        if len(instance_sequences) != len(set(instance_sequences.values())):
            raise RuntimeError("precondition check failed: instance_sequences contains duplicate sequence-ids")

        self.strategy_name = strategy_name
        self.instance_id = instance_id
        self.instance_sequences = instance_sequences
        self.max_sequence_id = max_sequence_id

    def get_sequence_id(self):

        # if current instance already has an assigned sequence, return it and this makes each it idempotent as well
        if self.instance_id in self.instance_sequences:
            return self.instance_sequences[self.instance_id]

        if self.strategy_name == 'last-used':
            return self.__choose_last_used_sequence_id__()
        elif self.strategy_name == 'first-available':
            return self.__choose_first_available_sequence_id__()
        else:
            logging.error('Unable to find appropriate strategy to execute for {0}'.format(self.strategy_name))
            raise NotImplementedError('Unable to find strategy to execute for {0}'.format(self.strategy_name))

    def __choose_last_used_sequence_id__(self):
        if len(list(self.instance_sequences.values())):
            max_assigned_sequence = max(self.instance_sequences.values())
        else:
            max_assigned_sequence = -1

        # if still less than max id, then increment & return
        if max_assigned_sequence < self.max_sequence_id:
            return max_assigned_sequence + 1

        # else find the smallest id & return
        return self.__choose_first_available_sequence_id__()

    def __choose_first_available_sequence_id__(self):

        # if no id is assigned, then return the first value in the sequence (0)
        if not self.instance_sequences:
            return 0

        # else find the smallest missing value not used
        complete_sequences = [i for i in range(self.max_sequence_id)]
        missing_values = [x for x in complete_sequences if x not in list(self.instance_sequences.values())]

        if not missing_values:
            logging.error('Unable to generate sequence_id since all ids are in use')
            raise OverflowError('Unable to generate sequence_id since all ids are in use')

        return sorted(missing_values)[0]
