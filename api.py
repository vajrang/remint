from datetime import datetime

import uvicorn
from fastapi import FastAPI

from datafetcher import ProcessorType, fetch_data_processor

app = FastAPI()


@app.get('/networth')
def get_networth():
    start = datetime.now()
    adp = fetch_data_processor(ProcessorType.ACCOUNTS)
    print(adp.get_summary())
    timetaken = datetime.now() - start
    print(f'{timetaken.total_seconds():.2f}')
    return adp.get_summary()


if __name__ == '__main__':
    uvicorn.run('api:app', reload=True)
