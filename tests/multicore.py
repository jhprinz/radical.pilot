import os
import sys
import radical.pilot

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!
#
# RADICAL-Pilot uses ssh to communicate with the remote resource. The 
# easiest way to make this work seamlessly is to set up ssh key-based
# authentication and add the key to your keychain so you won't be 
# prompted for a password. The following article explains how to set 
# this up on Linux:
#   http://www.cyberciti.biz/faq/ssh-password-less-login-with-dsa-publickey-authentication/


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# RCONF points to the resource configuration files.
#RCONF  = ["https://raw.github.com/radical-cybertools/radical.pilot/devel/configs/xsede.json",
#          "https://raw.github.com/radical-cybertools/radical.pilot/devel/configs/futuregrid.json"]
RCONF = "file:///Users/mark/proj/radical.pilot/configs/xsede.json"

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)

    if state == radical.pilot.states.FAILED:
        sys.exit(1)

#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == radical.pilot.states.FAILED:
        print "            Log: %s" % unit.log[-1]

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = radical.pilot.SSHCredential()
        session.add_credential(cred)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a pilot on stampede of in total 32 cores that spans two nodes,
        # runs for 15 mintutes and uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource  = "stampede.tacc.utexas.edu"
        pdesc.runtime   = 15 # minutes
        pdesc.cores     = 32 
        pdesc.cleanup   = False
        pdesc.pilot_agent_priv = "radical-pilot-agent-multicore.py"

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Number of cores for respective task
        #task_cores = [1 for _ in range(32)]
        task_cores = [3 for _ in range(32)]
        #task_cores = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]

        # Seconds of sleep for respective task
        task_sleep = [30 for _ in range(32)]

        compute_unit_descriptions = []
        for unit_no in range(32):
            cud = radical.pilot.ComputeUnitDescription()
            #cud.executable  = ""
            cud.arguments   = ["/bin/sleep %d && /bin/date > unit-%s.txt" % (task_sleep[unit_no], unit_no)]
            cud.cores       = task_cores[unit_no]

            compute_unit_descriptions.append(cud)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_unit_descriptions)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        for unit in units:
            print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
                   unit.stdout)

        session.close(delete=False)
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)