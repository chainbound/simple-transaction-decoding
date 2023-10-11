import asyncio
import json
import logging
import time

from web3 import Web3
from websockets import connect

logging.basicConfig(level=logging.INFO)
logging.getLogger('websockets').setLevel(logging.INFO)

fiber_ws_url = 'ws://beta.fiberapi.io:8545'

# Insert your api key
headers = {"Authorization": "your-key"}  # Add your access token here
params_full = '{"id":1,"jsonrpc":"2.0","method":"eth_subscribe","params":["newPendingTransactions", true]}'

w3 = Web3(Web3.HTTPProvider())


def token_list_filtering(chain):
    with open('token_metadata.json', 'r', encoding='utf-8') as f:
        token_data = json.load(f)

    eth_tokens = [
        {
            'coingeko_id': token['id'],
            'symbol': token['symbol'],
            'contract': token['platforms']['ethereum']
        }
        for token in token_data if 'platforms' in token and chain in token['platforms']
    ]

    list_contracts = [token['contract'] for token in eth_tokens]

    return eth_tokens, list_contracts


class TransactionDecoder:
    def __init__(self):
        with open('erc_20_abi.json', 'r') as f:
            self.erc_abi = json.load(f)

        self.erc_token = w3.eth.contract(abi=self.erc_abi)
        self.token_list, self.token_contracts_q = token_list_filtering('ethereum')
        self.master_list = []

    async def decode(self, trx_object):
        start_time = time.time_ns()

        # check if eth native transfer
        if trx_object['input'] == '0x':
            return await self._decode_eth_transfer(trx_object, start_time)

        # check if erc-20 token transfer/approval
        if 'to' in trx_object:
            if trx_object['to'].lower() in self.token_contracts_q:
                token_loc = self.token_contracts_q.index(trx_object['to'].lower())
                return await self._decode_erc_transfer(trx_object, token_loc, start_time)

    async def _decode_eth_transfer(self, trx_object, start_time):
        try:
            # TODO: fix values decimals (these can be fetched on-chain) or with Coingeko
            trx_meta = {
                'timestamp': start_time,
                'from': trx_object['from'],
                'to': trx_object['to'],
                'method': 'eth_transfer',
                'data': {'value': int(trx_object['value'], 16)},
                'symbol': 'eth'
            }
            logging.debug(f"{trx_object['hash']} -- {(time.time_ns() - start_time) / 10 ** 6}ms decoding time")
            return trx_meta
        except Exception as e:
            logging.debug(str(e))

    async def _decode_erc_transfer(self, trx_object, location, start_time):
        try:
            decoded_data = self.erc_token.decode_function_input(trx_object['input'])
            # TODO: fix values decimals (these can be fetched on-chain) or with Coingeko
            trx_meta = {
                'timestamp': start_time,
                'from': trx_object['from'],
                'to': trx_object['to'],
                'method': decoded_data[0],
                'data': decoded_data[1],
                'symbol': self.token_list[location]['symbol']
            }
            logging.debug(f"{trx_object['hash']} -- {(time.time_ns() - start_time) / 10 ** 6}ms decoding time")
            return trx_meta
        except Exception as e:
            logging.debug(str(e))


async def get_event(params):
    decoder = TransactionDecoder()
    async with connect(fiber_ws_url, extra_headers=headers) as ws:
        await ws.send(params)
        subscription_response = await ws.recv()
        logging.info(subscription_response)

        while True:
            message = await asyncio.wait_for(ws.recv(), timeout=15)
            response = json.loads(message)
            res = response['params']['result']
            decoded_trx = await decoder.decode(res)
            if decoded_trx:
                decoder.master_list.append(decoded_trx)
                if len(decoder.master_list) % 100 == 0:
                    # TODO save output in format / DB you desire
                    print(decoder.master_list)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    while True:
        loop.run_until_complete(get_event(params_full))
