import pandas as pd


og_data = pd.read_excel("PRICE UPLOAD.xlsx", sheet_name="Sheet1")

converted_data = []

for index, row in og_data.iterrows():
    if index == 0:
        print(row)
    converted_row = {
        "partid": row["Part #"],
        "Price Code": "RTL",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Retail"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "MAP",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["US MAP"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "DST",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Distributor"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    # Dealer Prices
    converted_row = {
        "partid": row["Part #"],
        "Price Code": "DLR",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Dealer\n(1-5 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "DLR",
        "Effect Date": "2025-07-28",
        "Order Qty": 6,
        "End Date": "2025-12-31",
        "Unit Price": row["Premier\n(6-24 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "DLR",
        "Effect Date": "2025-07-28",
        "Order Qty": 25,
        "End Date": "2025-12-31",
        "Unit Price": row["Silver\n(25-99 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "DLR",
        "Effect Date": "2025-07-28",
        "Order Qty": 100,
        "End Date": "2025-12-31",
        "Unit Price": row["Gold\n(100-249 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "DLR",
        "Effect Date": "2025-07-28",
        "Order Qty": 250,
        "End Date": "2025-12-31",
        "Unit Price": row["Platinum\n(250+ Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    # Gold Prices
    converted_row = {
        "partid": row["Part #"],
        "Price Code": "GLD",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Gold\n(100-249 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "GLD",
        "Effect Date": "2025-07-28",
        "Order Qty": 250,
        "End Date": "2025-12-31",
        "Unit Price": row["Platinum\n(250+ Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    # Platinum Prices
    converted_row = {
        "partid": row["Part #"],
        "Price Code": "PLT",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Platinum\n(250+ Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    # Premier Prices
    converted_row = {
        "partid": row["Part #"],
        "Price Code": "PMR",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Premier\n(6-24 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "PMR",
        "Effect Date": "2025-07-28",
        "Order Qty": 25,
        "End Date": "2025-12-31",
        "Unit Price": row["Silver\n(25-99 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "PMR",
        "Effect Date": "2025-07-28",
        "Order Qty": 100,
        "End Date": "2025-12-31",
        "Unit Price": row["Gold\n(100-249 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "PMR",
        "Effect Date": "2025-07-28",
        "Order Qty": 250,
        "End Date": "2025-12-31",
        "Unit Price": row["Platinum\n(250+ Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    # Silver Prices
    converted_row = {
        "partid": row["Part #"],
        "Price Code": "SIL",
        "Effect Date": "2025-07-28",
        "Order Qty": 1,
        "End Date": "2025-12-31",
        "Unit Price": row["Silver\n(25-99 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "SIL",
        "Effect Date": "2025-07-28",
        "Order Qty": 100,
        "End Date": "2025-12-31",
        "Unit Price": row["Gold\n(100-249 Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

    converted_row = {
        "partid": row["Part #"],
        "Price Code": "SIL",
        "Effect Date": "2025-07-28",
        "Order Qty": 250,
        "End Date": "2025-12-31",
        "Unit Price": row["Platinum\n(250+ Units)"],
        "Processed Flag": "N",
    }
    converted_data.append(converted_row)

converted = pd.DataFrame(converted_data)
converted["Unit Price"] = converted["Unit Price"].apply(lambda x: f"${x:,.2f}")
converted.to_excel("converted_price_upload.xlsx", index=False)
