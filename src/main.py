import json
import os
import requests
from dotenv import load_dotenv
from logger_config import setup_logger
from nats.aio.client import Client as NATS
from nats.js import api
import asyncio
from datetime import datetime

ALERT_SUBJECT = "alerts.>"   
ALERT_STREAM = "STREAM_ALERTS"   

ENGINE_SUBJECT = "engine.>"   
ENGINE_STREAM = "STREAM_ENGINE"   

async def main():

    logger = setup_logger()
    logger.info("Starting QuantFlow_NotifApp...")
    load_dotenv()
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    CHAT_ID_ALERTS = os.getenv("CHAT_ID_ALERTS")
    CHAT_ID_ENGINE = os.getenv("CHAT_ID_ENGINE")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


    nc = NATS()
    await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
    js = nc.jetstream()
    
    # --- Consumer 1: QuantFlow_Alert
    try:
        await js.delete_consumer(ALERT_STREAM, "Quant-Alert")
    except Exception:
        pass
        
    await js.add_consumer(
        ALERT_STREAM,
        api.ConsumerConfig(
            durable_name="Quant-Alert",
            filter_subject=ALERT_SUBJECT,
            ack_policy=api.AckPolicy.EXPLICIT,
            deliver_policy=api.DeliverPolicy.NEW,  
            max_ack_pending=5000,
        )
    )

    # --- Consumer 2: QuantFlow_Engine
    try:
        await js.delete_consumer(ALERT_STREAM, "Quant-Engine")
    except Exception:
        pass
        
    await js.add_consumer(
        ENGINE_STREAM,
        api.ConsumerConfig(
            durable_name="Quant-Engine",
            filter_subject=ENGINE_SUBJECT,
            ack_policy=api.AckPolicy.EXPLICIT,
            deliver_policy=api.DeliverPolicy.NEW,  
            max_ack_pending=5000,
        )
    )


    sub_NotifApp_alert = await js.pull_subscribe(ALERT_SUBJECT, durable="Quant-Alert")

    sub_NotifApp_engine = await js.pull_subscribe(ENGINE_SUBJECT, durable="Quant-Engine")

    async def alert_Notif():
        while True:
            try:
                msgs = await sub_NotifApp_alert.fetch(100, timeout=1)
                for msg in msgs:
                    logger.info(f"Received from {msg.subject}: ")

                    alert_data = json.loads(msg.data.decode("utf-8"))

                    #Send Message to Telegram
                    payload = {
                        "chat_id": CHAT_ID_ALERTS,
                        "text": "⛔️ ALERT Received:" + "\n\nReceived from: \n" + msg.subject+ "\n\nBody: \n" + alert_data
                    }
                    resp = requests.post(url, json=payload)

                    await msg.ack()
            except Exception as e:
                logger.error(
                        json.dumps({
                                "EventCode": -1,
                                "Message": f"NATS error: alert_Notif, {e}"
                            })
                    )
                await asyncio.sleep(0.05)
    
    logger.info(
            json.dumps({
                         "EventCode": 0,
                         "Message": f"Quant-Alert Subscriber starts...."
                    })
            )
       
    
   
    async def engine_Notif():
        while True:
            try:
                msgs = await sub_NotifApp_engine.fetch(100, timeout=1)
                for msg in msgs:
                    logger.info(f"Received from {msg.subject}: ")

                    engine_data = json.loads(msg.data.decode("utf-8"))

                    #Send Message to Telegram
                    payload = {
                        "chat_id": CHAT_ID_ENGINE,
                        "text": "✳️ Quant Engine:" + "\n\nReceived from: \n" + msg.subject+ "\n\nBody: \n" + engine_data
                    }
                    resp = requests.post(url, json=payload)

                    await msg.ack()
            except Exception as e:
                logger.error(
                        json.dumps({
                                "EventCode": -1,
                                "Message": f"NATS error: engine_Notif, {e}"
                            })
                    )
                await asyncio.sleep(0.05)
    
    logger.info(
            json.dumps({
                         "EventCode": 0,
                         "Message": f"Quant-Engine Subscriber starts...."
                    })
            )    


    await asyncio.gather(alert_Notif(), engine_Notif())
    
    
if __name__ == "__main__":
    asyncio.run(main())

    
