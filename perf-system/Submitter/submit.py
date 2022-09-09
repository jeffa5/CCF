#WARNING#
"""
This should exist inside your build directory
"""

import aiohttp
import asyncio
import ssl
# import requests

# url = "https://127.0.0.1:8000/app/log/private"

# headers = {"Content-Type": "application/json"}
# verify = './service_cert.pem'
# r = requests.post(url, data=data, headers=headers, verify=verify, cert=('./user0_cert.pem', 'user0_privk.pem'))
# print(r.text)




async def read():
    data = '{"id": 4, "msg": "Some arbitrary message payload DONEEE 4"}'
    sslcontext = ssl.create_default_context(cafile='./service_cert.pem')
    sslcontext.load_cert_chain('./user0_cert.pem', 'user0_privk.pem')
    async with aiohttp.ClientSession() as session:  
        for i in range(15):
            async with session.post('https://127.0.0.1:8000/app/log/private', data='{"id": '+ str(i+100)+', "msg": "Some arbitrary message payload'+str(i+100)+' "}', ssl=sslcontext) as resp:
                print(resp)

asyncio.run(read())