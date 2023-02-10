import json
import logging
import random
import time
import asyncio
import aiohttp
import argparse

from typing import Any
from asyncio import Task

from aiohttp import ClientResponse
from multidict import CIMultiDictProxy
from tqdm import tqdm

MILLION = 1000000


async def generate_payload(i):
    p = json.dumps([{"Content": f"{time.time()}_payload_it{i}_{j}"} for j in range(args.batch_size)])
    return p


# function to control the sleep time, returns number of seconds, by default 0
# random example:
#     sleep_time = lambda _: random.randint(1, 5)
sleep_time = lambda: random.randint(0, 1)

iterator = None


async def send_request(session, semaphore, url, headers, payload, retries=500):
    retried = 0
    while retried < retries:
        async with semaphore:
            # print(f"[{time.time()}]send_request: {payload}", flush=True)
            await asyncio.sleep(sleep_time())
            async with session.post(url, headers=headers, data=payload) as response:
                text = await response.text()
                if response.status == 200:
                    return response
                retried += 1
                logging.error(f"Failed to get a successful response, retrying... {retried}/{retries}")
                logging.error(f"response: {text}")
                logging.error(f"request body: {payload}")
    logging.error(f"Failed to get a successful response after {retries} attempts")
    return None


async def start_perf_test(url, headers, iteration):
    """
    Start the performance test, using the given url and headers, and the given number of iterations, each iteration
    will send a batch of requests, the number of which is specified by the batch_size argument, the total number of
    requests will be iteration * batch_size, the concurrency is specified by the concurrency argument, which is the
    maximum number of concurrent requests allowed.

    Args:
        url: the target url
        headers: the headers to use
        iteration: the number of iterations

    Returns:
        None
    """
    logging.basicConfig(encoding='utf-8', level=logging.DEBUG)
    logging.info(f"starting performance test with {iteration} iterations, {args.batch_size} per iteration")
    logging.info(f"target url: {url}, using headers: {headers}")
    semaphore = asyncio.Semaphore(args.concurrency)
    logging.info(f"args.concurrency limit: {args.concurrency}")
    async with aiohttp.ClientSession() as session:
        logging.info("preparing payloads...")
        payload_start = time.time()
        payload_tasks = [asyncio.create_task(generate_payload(i)) for i in range(iteration)]
        payloads = await asyncio.gather(*payload_tasks, return_exceptions=True)
        logging.info(f"{iteration} payloads prepared in {time.time() - payload_start} seconds")
        logging.info("starting benchmark, preparing tasks...")
        task_start = time.time()
        tasks: list[Task[Any]] = [asyncio.ensure_future(send_request(session, semaphore, url, headers, payload))
                                  for
                                  payload in payloads]
        logging.info(f"{iteration} tasks prepared in {time.time() - task_start} seconds")
        responses = []
        failed = []
        logging.info("starting benchmark, executing tasks...")
        execution_start = time.time()
        for index, future in enumerate(tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="stamp benchmark")):
            logging.info(f"{time.time()}processing task {index}")
            raw_response: ClientResponse = await future
            if raw_response is None:
                failed.append(raw_response)
                continue
            response_json = await raw_response.json()
            # if the type of result is dict
            result_is_dict = isinstance(response_json, dict)
            all_success_exists = "all_success" in response_json
            if result_is_dict and all_success_exists and response_json["all_success"]:
                responses.append(response_json)
            if (result_is_dict and not all_success_exists) or \
                    (result_is_dict and not response_json["all_success"]) or \
                    not result_is_dict:
                headers: CIMultiDictProxy = raw_response.headers
                request_id = headers.get("Apigw-Requestid")
                status = raw_response.status
                logging.error(f"failed to submit hash: {response_json}, request_id: {request_id}, status: {status}")
                failed.append(raw_response)
        if len(failed) > 0:
            logging.error(f"{len(failed)} of hashes failed")
        logging.info(f"{iteration} tasks executed in {time.time() - execution_start} seconds")
        return responses


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:8080",
                        help="API endpoint, default 'http://localhost:8080'")
    parser.add_argument("--env", default="dev", help="API environment, default 'dev'")
    parser.add_argument("--total", type=int, default=1 * MILLION,
                        help="Total number of hashes to submit, default 1 Million")
    parser.add_argument("--concurrency", type=int, default=10,
                        help="Concurrency limit, number of coroutines to run in parallel, default 10")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Batch size, number of hashes to submit in one request, default 100")
    args = parser.parse_args()

    iteration = int(args.total / args.batch_size)
    url = f"{args.endpoint}/{args.env}/submit-hash-batch"
    headers = {
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        'accept-language': "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        'Content-Type': 'application/json',
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78"
    }

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(start_perf_test(url, headers, iteration))
    loop.close()
