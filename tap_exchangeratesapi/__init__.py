#!/usr/bin/env python3

import sys
import time
from singer import utils

import requests
import singer
import backoff
import copy

from datetime import datetime, timedelta

base_url = "https://api.apilayer.com/exchangerates_data/{date}"

logger = singer.get_logger()
session = requests.Session()

DATE_FORMAT = '%Y-%m-%d'

REQUIRED_CONFIG_KEYS = [
    "apikey",
    "exchanges",
    "start_date",
    "schemaless"
]


def parse_response(r):
    flattened = r['rates']
    flattened[r['base']] = 1.0
    flattened['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(r['date'], DATE_FORMAT))
    return flattened


schema = {
    'type': 'object',
    'properties':
        {
            'date':
                {
                    'type': 'string',
                    'format': 'date-time'
                }
        }
}


def giveup(error):
    logger.error(error.response.text)
    response = error.response
    return not (response.status_code == 429 or
                response.status_code >= 500)


@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def request(url, params, headers):
    response = requests.get(url=url, params=params, headers=headers)
    response.raise_for_status()
    return response


def do_sync(config, start_date):
    exchanges = config["exchanges"]
    schemaless = config["schemaless"]

    state = {'start_date': start_date}
    next_date = start_date
    prev_schema = {}

    try:
        while datetime.strptime(next_date, DATE_FORMAT) <= datetime.utcnow():
            for exchange in exchanges:
                base = exchange["base"]
                symbols = exchange["symbols"]
                logger.info('Replicating exchange rate data from %s using base %s',
                            next_date,
                            base)

                response = request(base_url.format(date=next_date),
                                   {"base": base, "symbols": symbols},
                                   {"apikey": config["apikey"]})

                payload = response.json()

                # Update schema if new currency/currencies exist
                for rate in payload['rates']:
                    if rate not in schema['properties']:
                        schema['properties'][rate] = {'type': ['null', 'number']}

                # Only write schema if it has changed
                if schema != prev_schema:
                    singer.write_schema('exchange_rate', schema, 'date')

                if payload['date'] == next_date:
                    logger.info(f"schemaless : {schemaless}")
                    if schemaless:
                        singer.write_records('exchange_rate', [payload])
                    else:
                        singer.write_records('exchange_rate', [parse_response(payload)])

            state = {'start_date': next_date}
            next_date = (datetime.strptime(next_date, DATE_FORMAT) + timedelta(days=1)).strftime(DATE_FORMAT)
            prev_schema = copy.deepcopy(schema)

    except requests.exceptions.RequestException as e:
        logger.fatal('Error on ' + e.request.url +
                     '; received status ' + str(e.response.status_code) +
                     ': ' + e.response.text)
        singer.write_state(state)
        sys.exit(-1)

    singer.write_state(state)
    logger.info('Tap exiting normally')


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = {}

    logger.info(f"Sync Starting with exchange rates api.")
    if args.state:
        state.update(args.state)
    else:
        start_date = state.get('start_date') or args.config['start_date']
        start_date = singer.utils.strptime_with_tz(start_date).date().strftime(DATE_FORMAT)

        do_sync(args.config, start_date)
        logger.info("Sync Completed")


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        logger.critical(exc)
        raise exc
