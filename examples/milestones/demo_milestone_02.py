__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon
import time

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------
# Change these according to your needs 
CFG_USERNAME    = "oweidner"
CFG_RESOURCE    = "localhost"    
CFG_WORKING_DIR = "/tmp/sinon/"

#-------------------------------------------------------------------------------
#
def demo_milestone_02():
    """Demo for Milestone 2: Submit a single pilot to FutureGrid an execute 
    O(10) work units. Document performance.
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = sinon.SSHCredential()
        cred.user_id = CFG_USERNAME

        session.add_credential(cred)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Submit a 16-core pilot to india.futuregrid.org
        pd = sinon.ComputePilotDescription()
        pd.resource          = CFG_RESOURCE
        pd.working_directory = CFG_WORKING_DIR
        pd.cores             = 8
        pd.run_time          = 10 # minutes

        print "* Submitting pilot to '%s'..." % (pd.resource)
        p1 = pm.submit_pilots(pd)

        state = p1.wait(state=[sinon.states.RUNNING, sinon.states.FAILED])

        # If the pilot is in FAILED state it probably didn't start up properly. 
        if state == sinon.states.FAILED:
            print "  [ERROR] Pilot %s failed: %s." % (p1, p1.state_details[-1])
            sys.exit(-1)
        else:
            print "  [OK]    Pilot %s submitted successfully: %s." % (p1, p1.state_details[-1])

        # Create a workload of 64 '/bin/date' compute units
        compute_units = []
        for unit_count in range(0, 16):
            cu = sinon.ComputeUnitDescription()
            cu.cores = 1
            cu.executable = "/bin/date"
            compute_units.append(cu)


        # Combine the pilot, the workload and a scheduler via 
        # a UnitManager.
        um = sinon.UnitManager(session=session, scheduler="round_robin")
        um.add_pilots(p1)
        um.submit_units(compute_units)

        unit_list = um.list_units()
        print "* Submitted %s compute units: %s" % (len(unit_list), unit_list)

        # Wait for all compute units to finish.
        print "* Waiting for all compute units to finish..."
        um.wait_units()
        print "  FINISHED"

        # Cancel all pilots.
        pm.cancel_pilots()

        return 0

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(demo_milestone_02())

