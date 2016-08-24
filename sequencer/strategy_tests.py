import unittest
from strategy import SequenceStrategy

last_used_strategy = 'last-used'
first_available_strategy = 'first-available'


class SequenceStrategyTest(unittest.TestCase):

    def test_last_used_strategy(self):
        instance_sequences = {
            'i-1000': 0,
            'i-2000': 3,
            'i-3000': 7
        }
        strategy = SequenceStrategy(last_used_strategy, 'i-1234', instance_sequences, 8)
        self.assertIdempotentEqual(strategy.get_sequence_id(), 8)

    def test_first_available_strategy(self):
        instance_sequences = {
            'i-1000': 0,
            'i-2000': 3,
            'i-3000': 7
        }
        strategy = SequenceStrategy(first_available_strategy, 'i-1234', instance_sequences, 8)
        self.assertIdempotentEqual(strategy.get_sequence_id(), 1)

    def test_last_used_strategy_with_range_crossover(self):
        instance_sequences = {
            'i-1000': 0,
            'i-2000': 3,
            'i-3000': 8
        }
        strategy = SequenceStrategy(last_used_strategy, 'i-1234', instance_sequences, 8)
        self.assertIdempotentEqual(strategy.get_sequence_id(), 1)

    def test_first_available_strategy_for_complete_sequence(self):
        instance_sequences = {
            'i-1000': 0,
            'i-2000': 1,
            'i-3000': 2
        }
        strategy = SequenceStrategy(last_used_strategy, 'i-1234', instance_sequences, 8)
        self.assertIdempotentEqual(strategy.get_sequence_id(), 3)

    def test_first_available_strategy_with_overflow(self):
        instance_sequences = {
            'i-1000': 0,
            'i-2000': 1,
            'i-3000': 2
        }
        strategy = SequenceStrategy(first_available_strategy, 'i-1234', instance_sequences, 2)
        self.assertIdempotentRaises(OverflowError, strategy.get_sequence_id)

    def test_last_used_strategy_with_overflow(self):
        instance_sequences = {
            'i-1000': 0,
            'i-2000': 1,
            'i-3000': 2
        }
        strategy = SequenceStrategy(last_used_strategy, 'i-1234', instance_sequences, 2)
        self.assertIdempotentRaises(OverflowError, strategy.get_sequence_id)

    def test_unknown_strategy(self):
        self.assertIdempotentRaises(NotImplementedError, SequenceStrategy, 'dummy', None, {}, None)

    def test_duplicate_sequences_precondition(self):
        duplicate_instance_sequences = {
            'i-1000': 0,
            'i-2000': 0,
            'i-3000': 1
        }
        self.assertIdempotentRaises(RuntimeError, SequenceStrategy, None, None, duplicate_instance_sequences, None)

    def assertIdempotentEqual(self, expected, actual):
        self.assertEqual(expected, actual)
        self.assertEqual(expected, actual)

    def assertIdempotentRaises(self, error, callable_obj, *args):
        self.assertRaises(error, callable_obj, *args)
        self.assertRaises(error, callable_obj, *args)

if __name__ == '__main__':
    unittest.main()

