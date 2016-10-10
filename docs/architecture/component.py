

# ------------------------------------------------------------------------------
#
class SortWorker(multiprocessing.Process):

    # --------------------------------------------------------------------------
    #
    def __init__(input_queue, output_even_queue, output_odd_queue, update_queue):
    
        self._name              = 'SortWorker'
        self._input_queue       = input_queue       # from state SORTING
        self._output_even_queue = output_even_queue # into state EVEN
        self._output_odd_queue  = output_odd_queue  # into state ODD
        self._update_queue      = update_queue  
        self._terminate         = multiprocessing.Event()

        rpu.prof('start')


    # --------------------------------------------------------------------------
    #
    def stop():

        rpu.prof('stop requested')
        self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def run():

        rpu.prof('run')
        try:

            while not self._terminate:

                command, data = self._input_queue.get()

                if command == COMMAND_CANCEL :
                    self.stop()

                else command == COMMAND_SORT :

                    cu = data
                    cu.state = 'SORTING'

                    rpu.prof('get', msg="sorting_queue to sorter (%s)" \
                                       % cu.state, uid=cu['_id'])
                    self.sort(cu)

                else:
                    print 'unknown command %s - ignored' % command


        except:
            print('SortWorker failed')
            raise

        finally:
            rpu.prof('stop')


    # --------------------------------------------------------------------------
    #
    def sort(cu):

        try :

            # set state to the one this component is operating on, and
            # send state update
            cu.state = SORTING
            rpu.prof(uid=cu.uid, 'put', 'SortWorker to update_queue (SORTING)')
            self._update_queue.put(cu)

            # do the deed
            if not cu.counter % 2:

                # work!
                time.sleep(1)

                # set the state and push state update
                cu.state = PENDING_EVEN
                rpu.prof(uid=cu.uid, 'put', 'SortWorker to update_queue (PENDING_EVEN)')
                self._update_queue.put(cu)

                # push cu toward next component.
                rpu.prof(uid=cu.uid, 'put', 'SortWorker to output_queue_even (PENDING_EVEN)')
                self._output_queue_even.put(cu)

            else:

                # work!
                time.sleep(2)

                # set the state and push state update
                cu.state = PENDING_ODD
                rpu.prof(uid=cu.uid, 'put', 'SortWorker to update_queue (PENDING_ODD)')
                self._update_queue.put(cu)

                # ... and push toward next component.
                rpu.prof(uid=cu.uid, 'put', 'SortWorker to output_queue_odd (PENDING_ODD)')
                self._output_queue_odd.put(cu)


        except Exception as e:

            # on failuer, just push a state update -- don't advance the unit
            cu.state = FAILED
            rpu.prof(uid=cu.uid, 'put', 'SortWorker to update_queue (FAILED)')
            self._update_queue.put(cu)

#
# ------------------------------------------------------------------------------


