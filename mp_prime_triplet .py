"""
Name: Bria Weisblat
Date: 10/14/24
Assignment: Assignment #3
Due Date: 10/14/24
About this project: This program computes the smallest prime triplets where the values of the
primes are no less than N (a value input by the user) using a number of processors specified by the user.
Assumptions: Assume there are four processors being used if no input is given at the comman line.

Timing results in linprog:

Input: 1000000000000 Nprocs: 1
Time to build the list of prime numbers: 3.9662394523620605 seconds
Time to search for prime triplet: 5.404426097869873 seconds
Total time: 9.370678663253784 seconds

Input: 1000000000000 Nprocs: 2
Time to build the list of prime numbers: 3.1149284839630127 seconds
Time to search for prime triplet: 2.8665783405303955 seconds
Total time: 5.981515884399414 seconds

Input: 1000000000000 Nprocs: 4
Time to build the list of prime numbers: 2.518954038619995 seconds
Time to search for prime triplet: 1.3175244331359863 seconds
Total time: 3.8364908695220947 seconds

Input: 1000000000000 Nprocs: 8
Time to build the list of prime numbers: 2.287447452545166 seconds
Time to search for prime triplet: 0.6712582111358643 seconds
Total time: 2.9587182998657227 seconds

Input: 3000000000000 Nprocs: 1
Time to build the list of prime numbers: 8.016845703125 seconds
Time to search for prime triplet: 7.110996723175049 seconds
Total time: 15.127853870391846 seconds

Input: 3000000000000 Nprocs: 2
Time to build the list of prime numbers: 6.20095682144165 seconds
Time to search for prime triplet: 3.520214557647705 seconds
Total time: 9.72118353843689 seconds

Input: 3000000000000 Nprocs: 4
Time to build the list of prime numbers: 5.050644159317017 seconds
Time to search for prime triplet: 1.85630202293396 seconds
Total time: 6.906966686248779 seconds

Input: 3000000000000 Nprocs: 8
Time to build the list of prime numbers: 4.470065593719482 seconds
Time to search for prime triplet: 0.8768093585968018 seconds
Total time: 5.346887588500977 seconds
"""

import math
import queue
import sys
import time
import multiprocessing
from multiprocessing import Event

primes = [2, 3, 5]

def isPrime(n, primes):
    """
    This function assumes that primes is sorted and includes
    all prime numbers up to at least int(math.sqrt(n)) + 1
    """

    i = 0
    b = int(math.sqrt(n)) + 1
    pLen = len(primes)
    while i < pLen and (primes[i] < b):
        if (n % primes[i] == 0):
            return False
        i = i + 1
    return True


# Receives a tuple (start, end) from tQueue and sends lists of primes back to the rQueue.
def find_primes_worker(id, tQueue, rQueue, primes, nprocs):
    # If the worker ID is bad
    if (id < 0) or (id >= nprocs):
        # Print an error statement and return
        print(f"Worker id({id}) is not between 0 and {nprocs - 1}.")
        return

    # Keep the worker alive to continually receive tasks
    while True:
        # Get the lower and upper bounds
        task = tQueue.get()
        # Check for the exit signal
        if task is None:
            break
        # Put the range into the task variable
        l, u = task
        # List to store the primes
        myPrimes = []
        # Iterate through the assigned range
        for i in range(l, u):
            # Check for primes
            if isPrime(i, primes):
                # Add primes to the list
                myPrimes.append(i)
        # Get the total number of primes found
        pLen = len(myPrimes)

        # Send primes in groups of 10
        for i in range(0, pLen // 10):
            rQueue.put(myPrimes[i * 10:(i + 1) * 10])

        # If there are leftover primes
        if pLen // 10 * 10 != pLen:
            # Send the leftover primes
            rQueue.put(myPrimes[pLen // 10 * 10:pLen])
    # Stop sending things
    rQueue.put([-1])


def makePrimeList(n, nprocs, primes):
    smallPrime = int(math.sqrt(n)) + 1
    for i in range(6, n):
        if isPrime(i, primes):
            primes.append(i)

    # Queue for distributing ranges to worker
    tQueues = [multiprocessing.Queue() for _ in range(nprocs)]

    # Queue for collecting prime list from workers
    rQueues = [multiprocessing.Queue() for _ in range(nprocs)]

    # A list of all worker processes
    processes = []

    # For nprocs processes
    for i in range(nprocs):
        # Create each process
        p = multiprocessing.Process(target=find_primes_worker, args=(i, tQueues[i], rQueues[i], primes, nprocs))
        processes.append(p)
        p.start()

    # Calculate the range for each worker
    chunk = (n - smallPrime) // nprocs + 1

    # For each worker
    for i in range(nprocs):
        start = smallPrime + i * chunk
        end = min(start + chunk, n)
        tQueues[i].put((start, end))

    # Send a termination signal to each worker
    for i in range(nprocs):
        tQueues[i].put(None)  # Sending None to signal workers to exit

    # For each worker
    for i in range(nprocs):
        flag = True
        while flag:
            primes_from_worker = rQueues[i].get()
            if primes_from_worker[-1] == -1:
                primes.extend(primes_from_worker[:-1])
                flag = False
            else:
                primes.extend(primes_from_worker)

    for p in processes:
        p.join()


# New find triplets function
def find_triplets_worker(task_queue, result_queue, terminate, primes):
    """Worker function to find prime triplets."""
    while not terminate.is_set():
        try:
            # we don't know if there'll actually be a task or not, so "try" it
            start, end = task_queue.get(timeout=0.1)
        except multiprocessing.queues.Empty:
            continue

        found_primes = []
        for cur in range(start, end + 1):
            # loop through range and add primes
            if isPrime(cur, primes):
                # add prime to list
                found_primes.append(cur)
                if len(found_primes) >= 3:
                    # only check for a triplet when there's more than three found_primes AND when a new prime is found
                    # use python slicing features to always get the most recent three primes
                    n1, n2, n3 = found_primes[-3:]
                    if n3 - n1 == 6:
                        result_queue.put([n1, n2, n3])
                        terminate.set()  # send stop signal to other workers
                        return

        result_queue.put(None)


if __name__=="__main__":

    nprocs = 4
    if len(sys.argv) > 1:
        nprocs = int(sys.argv[1])

    batch_size = 200

    n = int(input("Input n: "))
    startT = time.time()

    # assume the primes in the prime triplet are not larger than n + 100000
    largestPrimeNeeded = int(math.sqrt(n + 100000))

    if (n <= 5):
        msg = 'The smallest triplet larger than ' + str(n) + ' is ('
        msg = msg + '5, 7, 11)'
        print(msg)
        exit()

    listStartT = time.time()
    makePrimeList(largestPrimeNeeded, nprocs, primes)
    listStopT = time.time()

    searchStartT = time.time()

    # create queues and event
    # instead of pulling from multiple queues like the makePrimeList, we want this all to contribute to the same task
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    terminate = multiprocessing.Event()

    # start worker processes
    processes = []

    for i in range(nprocs):
        p = multiprocessing.Process(target=find_triplets_worker, args=(task_queue, result_queue, terminate, primes))
        processes.append(p)
        p.start()

    # this function for batch size was found via experimentation, this tends to work well across most n and nprocs
    # max(50, int(math.log(n) * 500 / nprocs))
    # distribute tasks to the task queue
    current = n
    while current <= n + 100000:
        task_queue.put((current, current + batch_size))
        current += batch_size

    # repeatedly check for a result in the result queue
    found = False
    while not found:
        result = result_queue.get()
        if result is not None:
            print(f"Final Triplet found: {result}")
            found = True
    stopT = time.time()


    joinT = time.time()
    # kill off remaining workers, ok to forcibly terminate bc we already got the result
    terminate.set()
    for p in processes:
        p.terminate()
    joinEndT = time.time()

    print()
    print(f'Time to build the list of prime numbers: {listStopT - listStartT} seconds')
    print(f'Time to search for prime triplet: {stopT - searchStartT} seconds')
    print(f'Total time: {stopT - startT} seconds')
