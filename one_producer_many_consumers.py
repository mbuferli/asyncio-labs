import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import random
from driver import *
VALID_IDS = [1, 2, 3, 4]
MONITOR_SLEEP = 20
PRODUCER_SLEEP = 20
MAX_FETCHED_COUNT = 5


async def monitor(queues, loop):
    while True:
        print("> [MONITOR] Turn ...")
        for id in VALID_IDS:
            print("> [MONITOR] Q%s[%s]" % (id, queues[id].qsize()))
        print("> [MONITOR] Sleep.")
        await asyncio.sleep(MONITOR_SLEEP, loop=loop)


async def producer(queues, loop):
    while True:
        print("> [PRODUCER] Turn ...")
        queued_count = 0
        for match_id in get_waiting_matches()[0: MAX_FETCHED_COUNT]:
            queue_id = (match_id % len(VALID_IDS) + 1)
            queues[queue_id].put_nowait(match_id)
            set_match_as_queued(match_id)  # mark as queued in DB
            queued_count = queued_count + 1
        print("> [PRODUCER] Added new %s. Sleep." % queued_count)
        await asyncio.sleep(PRODUCER_SLEEP, loop=loop)


async def consumer(q, loop):
    while True:
        print("\n> [CONSUMER] Turn ...")
        match_id = await q.get()
        print("> [CONSUMER] Process <%s> ..." % match_id)
        proc_count = set_match_processed(match_id)  # fake processing, just decrement a counter
        await asyncio.sleep(random.randint(1, 5), loop=loop)  # add delay
        if proc_count > 0:
            q.put_nowait(match_id)
            print("> [CONSUMER] Re-QUEUE <%s>" % match_id)
        else:
            print("> [CONSUMER] Finished <%s>" % match_id)


if __name__ == "__main__":
    # verbose life style
    print("=== ASYNCIO Demo Started ===\n")
    print("> [MAIN] Match ids: %s" % get_waiting_matches())

    # init scheduler and reset data
    scheduler = AsyncIOScheduler()
    reset_data()

    loop = asyncio.get_event_loop()

    # init queues
    QUEUES = {
        1: asyncio.Queue(),  # (match_id % 4 + 1) = 1
        2: asyncio.Queue(),  # (match_id % 4 + 1) = 2
        3: asyncio.Queue(),  # (match_id % 4 + 1) = 3
        4: asyncio.Queue(),  # (match_id % 4 + 1) = 4
    }

    loop.create_task(producer(QUEUES, loop))
    loop.create_task(monitor(QUEUES, loop))
    for id in VALID_IDS:
        loop.create_task(consumer(QUEUES[id], loop))  # pass to each consumer, the relative queues

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        loop.close()


