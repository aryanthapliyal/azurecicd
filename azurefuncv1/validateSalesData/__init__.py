import logging
import azure.functions as func
import pandas as pd
import io

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        csv_data = req.get_body()
        if not csv_data:
            return func.HttpResponse(
                "Please pass the CSV data in the request body",
                status_code=400
            )

        df = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))
        errors = []
        required_columns = ['TransactionID', 'ProductName', 'Amount']

        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            return func.HttpResponse(f"Invalid Data: Missing columns - {', '.join(missing_cols)}", status_code=200)

        for index, row in df.iterrows():
            for col in required_columns:
                if pd.isna(row[col]) or str(row[col]).strip() == '':
                    errors.append(f"Row {index + 2}: Missing value for {col}")

            if not pd.isna(row['Amount']) and str(row['Amount']).strip() != '':
                try:
                    amount = float(row['Amount'])
                    if amount < 0:
                        errors.append(f"Row {index + 2}: Negative amount ({amount}) is not allowed.")
                except (ValueError, TypeError):
                    errors.append(f"Row {index + 2}: Invalid format for Amount ('{row['Amount']}')")

        if errors:
            error_message = "Invalid Data: \n" + "\n".join(errors)
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=200)
        else:
            logging.info("Validation Passed")
            return func.HttpResponse("Validation Passed", status_code=200)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse(f"Invalid Data: Error processing file - {str(e)}", status_code=200)
