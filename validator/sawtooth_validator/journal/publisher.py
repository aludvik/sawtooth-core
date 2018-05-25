# Copyright 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

# pylint: disable=inconsistent-return-statements

import abc
import logging
import queue
from threading import RLock

from sawtooth_validator.concurrent.thread import InstrumentedThread
from sawtooth_validator.execution.scheduler_exceptions import SchedulerError


from sawtooth_validator import metrics

import ctypes
from enum import IntEnum

from sawtooth_validator.ffi import PY_LIBRARY
from sawtooth_validator.ffi import CommonErrorCode
from sawtooth_validator.ffi import OwnedPointer

LOGGER = logging.getLogger(__name__)


class ConsensusNotReady(Exception):
    """
    Consensus is not ready to build a block.
    """


class NoPendingBatchesRemaining(Exception):
    """There are no pending batches remaining."""


class FinalizeBlockResult:
    def __init__(self, block, remaining_batches, last_batch, injected_batches):
        self.block = block
        self.remaining_batches = remaining_batches
        self.last_batch = last_batch
        self.injected_batches = injected_batches


class PendingBatchObserver(metaclass=abc.ABCMeta):
    """An interface class for components wishing to be notified when a Batch
    has begun being processed.
    """

    @abc.abstractmethod
    def notify_batch_pending(self, batch):
        """This method will be called when a Batch has passed initial
        validation and is queued to be processed by the Publisher.

        Args:
            batch (Batch): The Batch that has been added to the Publisher
        """
        raise NotImplementedError('PendingBatchObservers must have a '
                                  '"notify_batch_pending" method')


class _PublisherThread(InstrumentedThread):
    def __init__(self, block_publisher, batch_queue,
                 check_publish_block_frequency):
        super().__init__(name='_PublisherThread')
        self._block_publisher = block_publisher
        self._batch_queue = batch_queue
        self._check_publish_block_frequency = \
            check_publish_block_frequency
        self._exit = False

    def run(self):
        try:
            # make sure we don't check to publish the block
            # to frequently.
            next_check_publish_block_time = time.time() + \
                self._check_publish_block_frequency
            while True:
                try:
                    self._batch_queue.get(
                        timeout=self._check_publish_block_frequency,
                        and_then=self._block_publisher.on_batch_received)
                except queue.Empty:
                    # If getting a batch times out, just try again.
                    pass

                if next_check_publish_block_time < time.time():
                    self._block_publisher.on_check_publish_block()
                    next_check_publish_block_time = time.time() + \
                        self._check_publish_block_frequency
                if self._exit:
                    return
        # pylint: disable=broad-except
        except Exception as exc:
            LOGGER.exception(exc)
            LOGGER.critical("BlockPublisher thread exited with error.")

    def stop(self):
        self._exit = True


class BlockPublisher(OwnedPointer):
    """
    Responsible for generating new blocks and publishing them when the
    Consensus deems it appropriate.
    """

    def __init__(self,
                 transaction_executor,
                 block_cache,
                 state_view_factory,
                 settings_cache,
                 block_sender,
                 batch_sender,
                 chain_head,
                 identity_signer,
                 data_dir,
                 config_dir,
                 permission_verifier,
                 check_publish_block_frequency,
                 batch_observers,
                 batch_injector_factory=None):
        """
        Initialize the BlockPublisher object

        Args:
            transaction_executor (:obj:`TransactionExecutor`): A
                TransactionExecutor instance.
            block_cache (:obj:`BlockCache`): A BlockCache instance.
            state_view_factory (:obj:`StateViewFactory`): StateViewFactory for
                read-only state views.
            block_sender (:obj:`BlockSender`): The BlockSender instance.
            batch_sender (:obj:`BatchSender`): The BatchSender instance.
            chain_head (:obj:`BlockWrapper`): The initial chain head.
            identity_signer (:obj:`Signer`): Cryptographic signer for signing
                blocks
            data_dir (str): path to location where persistent data for the
                consensus module can be stored.
            config_dir (str): path to location where configuration can be
                found.
            batch_injector_factory (:obj:`BatchInjectorFatctory`): A factory
                for creating BatchInjectors.
        """
        super(BlockPublisher, self).__init__('block_publisher_drop')

        # TODO: This probably needs to return a handle to an object that
        # batches can be sent on, which would replace queue_batch
        assert PY_LIBRARY.call(
            'block_publisher_new',
            ctypes.py_object(transaction_executor),
            ctypes.py_object(block_cache),
            ctypes.py_object(state_view_factory),
            ctypes.py_object(settings_cache),
            ctypes.py_object(block_sender),
            ctypes.py_object(batch_sender),
            ctypes.py_object(chain_head),
            ctypes.py_object(identity_signer),
            ctypes.py_object(data_dir),
            ctypes.py_object(config_dir),
            ctypes.py_object(permission_verifier),
            ctypes.py_object(check_publish_block_frequency),
            ctypes.py_object(batch_observers),
            ctypes.py_object(batch_injector_factory),
            ctypes.byref(self.pointer)) == CommonErrorCode.Success

    def _call(self, method, *args):
        # TODO: Replace with error code handling
        assert PY_LIBRARY.call(
            'block_publisher_' + method,
            self.pointer,
            *args) == CommonErrorCode.Success

    def start(self):
        self._call('start')

    def stop(self):
        self._call('stop')

    # TODO:
    # def queue_batch(self, batch):
    #     """
    #     New batch has been received, queue it with the BlockPublisher for
    #     inclusion in the next block.
    #     """
    #     self._batch_queue.put(batch)
    #     for observer in self._batch_observers:
    #         observer.notify_batch_pending(batch)

    def pending_batch_info(self):
        """Returns a tuple of the current size of the pending batch queue
        and the current queue limit.
        """
        c_length = ctypes.c_size_t(0)
        c_limit = ctypes.c_size_t(0)
        self._call(
            'pending_batch_info',
            ctypes.byref(c_length),
            ctypes.byref(c_limit))

        return (c_length, c_limit)

    # TODO:
    # @property
    # def chain_head_lock(self):
    #     return self._lock

    def on_chain_updated(self, chain_head,
                         committed_batches=None,
                         uncommitted_batches=None):
        """
        The existing chain has been updated, the current head block has
        changed.

        :param chain_head: the new head of block_chain, can be None if
        no block publishing is desired.
        :param committed_batches: the set of batches that were committed
         as part of the new chain.
        :param uncommitted_batches: the list of transactions if any that are
        now de-committed when the new chain was selected.
        :return: None
        """
        try:
            self._call(
                'on_chain_updated',
                ctypes.py_object(chain_head),
                ctypes.py_object(committed_batches),
                ctypes.py_object(uncommitted_batches))

        # pylint: disable=broad-except
        except Exception:
            LOGGER.exception(
                "Unhandled exception in BlockPublisher.on_chain_updated")

    def has_batch(self, batch_id):
        has = ctypes.bool(False)
        c_batch_id = ctypes.c_char_p(batch_id.encode())

        self._call(
            'has_batch',
            c_batch_id,
            ctypes.byref(has))

        return has
