import numpy as np
import pandas as pd
import os
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data import DateRange
from google.analytics.data import Dimension
from google.analytics.data import Metric
from google.analytics.data import RunReportRequest
from google.analytics.data import OrderBy



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "put the path to the json key file"
web_property_id = 'put your property id'
client = BetaAnalyticsDataClient()

start_date = input('Enter the desired starting date as the following example 2023-09-24: ')
end_date = input('Enter the desired ending  date as the following example 2023-10-24: ')

date_range = pd.date_range(start=start_date, end=end_date)

def format_report(request):
    try:
        response = client.run_report(request)

        # Row index
        row_index_names = [header.name for header in response.dimension_headers]
        row_header = []
        for i in range(len(row_index_names)):
            row_header.append([row.dimension_values[i].value for row in response.rows])

        row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names=np.array(row_index_names))

        # Row Flat data
        metric_names = [header.name for header in response.metric_headers]
        data_values = []
        for i in range(len(metric_names)):
            data_values.append([row.metric_values[i].value for row in response.rows])

        output = pd.DataFrame(data=np.transpose(np.array(data_values, dtype='f')), index=row_index_named, columns=metric_names)

        
        return output
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

web_report_configs = [
    {
        "name": "raw_data",
        "dimensions": [Dimension(name="date"), Dimension(name="eventName"), Dimension(name='sessionSourceMedium'), Dimension(name='sessionCampaignName')],
        "metrics": [Metric(name="sessions")],
    },
    '''
    Define As many reports as needed using the proper dimensions and metrics
    '''
]

report_results = {}

for date in date_range:
    for report_config in web_report_configs:
        start_date = date.strftime('%Y-%m-%d')
        end_date = date.strftime('%Y-%m-%d')
        web_request = RunReportRequest(
            property='properties/' + web_property_id,
            dimensions=report_config["dimensions"],
            metrics=report_config["metrics"],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        )
        report_name = report_config["name"]
        report_df = format_report(web_request)

        if report_df is not None:
            output_directory = "./raw_data1"

            if not os.path.exists(output_directory):
                os.makedirs(output_directory)
                print('Creating folder:', output_directory)
                print('===============================================')
            else:
                os.makedirs(output_directory, exist_ok=True)
                print('output folder already exists', output_directory)
                print('===============================================')

            file_name = f'Raw_Sessions_KSA_{start_date}.csv'
            file_path = os.path.join(output_directory, file_name)

            if os.path.exists(file_path):
                choice = input(f'The file {file_name} already exists. Do you want to (O)verwrite or (C)ontinue? [O/C]: ').strip().lower()
                if choice == 'o':
                    print(f'Overwriting the existing file {file_name}')
                    report_df.to_csv(file_path)
                elif choice == 'c':
                    print('Continuing with the existing file.')
                else:
                    print('Invalid choice. Please enter "O" to overwrite or "C" to continue.')
            else:
                report_df.to_csv(file_path)
                print(f'Saving the file as {file_name}')