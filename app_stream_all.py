from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from fastapi import FastAPI
from pandasql import sqldf
import xlsx_streaming
import pandas as pd
import uvicorn
import random
import io


# Initialize FastAPI app
app = FastAPI()


# Dummy dataframe
gen_data = [[random.random() for _ in range(3)] for _ in range(15)]

df = pd.DataFrame(gen_data, columns=['un', 'deux', 'trois'])


# Select the dataframe with SQL to imitate a Databae request
def select_table():
    truc = sqldf(f'''
        SELECT
            *
        FROM
            df
        ORDER BY
            un
        ''')
    return truc


# Generate header and first row for xlsx_streaming data typing
async def generate_header_buffer(data):

    # request data as whole dataframe
    snow_data = data
    headers = snow_data.columns

    book = Workbook()
    sheet = book.active
    sheet.append(list(headers))
    sheet.append(list(snow_data.iloc[0]))
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer


# Stream the data in batches
async def generate_excel(data, buffer):
    for batch in xlsx_streaming.stream_queryset_as_xlsx(qs=data.itertuples(index=False), xlsx_template=buffer, batch_size=1):
        yield batch


# -----------------------------------------------------------------------------------------------------

# Using StreamingResponse to stream the data
@app.get("/extract", response_class=StreamingResponse)
async def extract(filename: str):

    data = select_table()
    buffer = await generate_header_buffer(data)

    return StreamingResponse(
        content=generate_excel(data, buffer),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={
            "Content-Disposition": f"attachment; filename={filename}.xlsx"}
    )


# run the app locally
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8001)
