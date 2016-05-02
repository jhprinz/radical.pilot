
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import UMGRSchedulingComponent, ROLE, ADDED


# ==============================================================================
#
class RoundRobin(UMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._wait_pool = list()             # set of unscheduled units
        self._wait_lock = threading.RLock()  # look on the above set

        self._pids = list()
        self._idx  = 0


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):

        # pilots just got added.  If we did not have any pilot before, we might
        # have units in the wait queue waiting -- now is a good time to take
        # care of those!
        with self._wait_lock:

            self._pids += pids

            if self._wait_pool:
                units = self._wait_pool[:]   # deep copy to avoid data recursion
                self._wait_pool = list()
                self._schedule_units(units)


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):

        with self._pilots_lock:

            for pid in pids:

                if not pid in self._pids:
                    raise ValueError('no such pilot %s' % pid)

                self._pids.remove(pid)
                # FIXME: cancel units


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_SCHEDULING, publish=True, push=False)
        self._schedule_units(units)


    # --------------------------------------------------------------------------
    #
    def _schedule_units(self, units):

        with self._pilots_lock:

            if not self._pids:

                # no pilots, no schedule...
                with self._wait_lock:
                    for unit in units:
                        self._prof.prof('wait', uid=unit['uid'])
                    self._wait_pool += units
                    return

            for unit in units:

                # determine target pilot for unit
                if self._idx >= len(self._pids):
                    self._idx = 0

                pid   = self._pids[self._idx]
                pilot = self._pilots[pid]['pilot']

                self._idx += 1

                # we assign the unit to the pilot.
                # this is also a good opportunity to determine the unit sandbox
                unit['pilot']   = pid
                unit['sandbox'] = self._session._get_unit_sandbox(unit, pilot)
    
            # advance all units
            self.advance(units, rps.UMGR_STAGING_INPUT_PENDING, 
                         publish=True, push=True)
        

# ------------------------------------------------------------------------------
