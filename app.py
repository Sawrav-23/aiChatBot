from dotenv import load_dotenv
import streamlit as st
import os
import psycopg2
import google.generativeai as genai
import pandas as pd
import re

st.set_page_config(page_title="AccoBuddy")

load_dotenv()

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


# Function to load Google Gemini model and provide SQL query as a response
def get_gemini_response(question, prompt, schema_name):
    # Format the prompt with the schema name
    formatted_prompt = prompt.format(schema_name=schema_name)
    model = genai.GenerativeModel("gemini-pro")
    response = model.generate_content([formatted_prompt, question])
    sql_query = response.text
    # Remove any backticks and sql word from the SQL query
    sql_query = sql_query.replace("```", "").strip()
    sql_query = sql_query.replace("sql", "").strip()

    return sql_query


# Function to retrieve query from PostgreSQL database with schema filtering
def read_sql_query(sql, db_url, schema_name):
    # Modify the SQL query to prepend the schema name to table references
    modified_sql = sql.replace(
        "ac_e52fec", schema_name
    )  # Replace with the provided schema name

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    try:
        cur.execute(modified_sql)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]  # Get column names
        conn.commit()
        return rows, colnames, modified_sql
    except Exception as e:
        conn.rollback()
        return str(e), None, modified_sql
    finally:
        conn.close()


def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


local_css("style.css")

# Define Your Prompt
prompt = """
You are an expert in converting English questions into SQL (PostgreSQL) queries! You have a deep understanding of various database schemas and are particularly skilled in interpreting user queries to generate precise SQL statements. Your focus is on ensuring that the generated queries are efficient, logically structured, and accurate according to the provided tables and relationships.

Your task is to convert a provided English question into an SQL query. Here’s the question I would like you to convert:
- Question:

Keep in mind the database schema I have shared, specifically ensuring that any references to tables use the appropriate schema name `{schema_name}` and that foreign key relations are accurately represented. Be cautious about SQL syntax, including the correct use of JOINs, WHERE clauses, and any necessary GROUP BY or ORDER BY clauses based on the nature of the question.

The tables and their metadata are:

- `{schema_name}.mst_godown`
  - **Purpose**: Stores information about godowns (warehouses).
  - **Columns**:
    - `guid`: Unique identifier for the godown (Primary Key).
    - `alterid`: Alternative ID for the godown.
    - `name`: Name of the godown.
    - `parent`: Parent godown if any.
    - `_parent`: Foreign key reference to a parent godown.
    - `address`: Address of the godown.

- `{schema_name}.trn_inventory`
  - **Purpose**: Records inventory transactions.
  - **Columns**:
    - `guid`: Unique identifier for the transaction (Primary Key).
    - `item`: Item being transacted.
    - `_item`: Foreign key reference to the item.
    - `quantity`: Quantity of the item.
    - `rate`: Rate of the item.
    - `amount`: Total amount for the transaction.
    - `additional_amount`: Any additional amount associated with the transaction.
    - `discount_amount`: Discount applied to the transaction.
    - `godown`: Godown where the transaction took place.
    - `_godown`: Foreign key reference to the godown.
    - `tracking_number`: Tracking number for the transaction.
    - `order_number`: Associated order number.
    - `order_duedate`: Due date for the order.

- `{schema_name}.mst_stock_item`
  - **Purpose**: Contains details of stock items.
  - **Columns**:
    - `guid`: Unique identifier for the stock item (Primary Key).
    - `alterid`: Alternative ID for the stock item.
    - `name`: Name of the item.
    - `parent`: Parent of the items group name.
    - `_parent`: Foreign key reference to a parent group.
    - `alias`: Alias or alternate name for the item.
    - `uom`: Unit of measurement for the item.
    - `_uom`: Foreign key reference to the unit of measurement.
    - `opening_balance`: Initial stock balance.
    - `opening_rate`: Rate at which the stock was opened.
    - `opening_value`: Value of the opening stock.
    - `gst_nature_of_goods`: GST nature of the goods.
    - `gst_hsn_code`: HSN code for GST.
    - `gst_taxability`: GST taxability.
    - `closing_balance`: Stock balance at the end of the period.
    - `closing_value`: Value of the closing stock.
    - `last_sale_party`: Last party to whom the item was sold.
    - `last_purc_party`: Last party from whom the item was purchased.
    - `closing_rate`: Rate at which the stock was closed.
    - `reorder_level`: Minimum stock level for reorder.
    - `category`: Category of the stock item.
    - `last_sale_date`: Date of the last sale.
    - `last_purc_date`: Date of the last purchase.
    - `last_sale_price`: Price of the last sale.
    - `last_sale_quantity`: Quantity of the last sale.
    - `last_purc_price`: Price of the last purchase.
    - `last_purc_quantity`: Quantity of the last purchase.

- `{schema_name}.trn_voucher`
  - **Purpose**: Records details of financial vouchers.
  - **Columns**:
    - `guid`: Unique identifier for the voucher (Primary Key).
    - `alterid`: Alternative ID for the voucher.
    - `date`: Date of the voucher.
    - `voucher_type`: Type of the voucher.
    - `_voucher_type`: Foreign key reference to the voucher type.
    - `voucher_number`: Number of the voucher.
    - `reference_number`: Reference number for the voucher.
    - `reference_date`: Date of the reference.
    - `narration`: Description or narration of the voucher.
    - `party_name`: Name of the party associated with the voucher.
    - `_party_name`: Foreign key reference to the party.
    - `place_of_supply`: Place where the supply is made.
    - `is_invoice`: Indicates if the voucher is an invoice.
    - `is_accounting_voucher`: Indicates if the voucher is an accounting voucher.
    - `is_inventory_voucher`: Indicates if the voucher is an inventory voucher.
    - `is_order_voucher`: Indicates if the voucher is an order voucher.
    - `masterid`: Master ID related to the voucher.

- `{schema_name}.trn_bill`
  - **Purpose**: Contains billing details.
  - **Columns**:
    - `guid`: Unique identifier for the bill (Primary Key).
    - `ledger`: Ledger account associated with the bill.
    - `_ledger`: Foreign key reference to the ledger account.
    - `name`: Name of the bill.
    - `amount`: Amount of the bill.
    - `billtype`: Type of the bill.

- `{schema_name}.mst_vouchertype`
  - **Purpose**: Defines different types of vouchers.
  - **Columns**:
    - `guid`: Unique identifier for the voucher type (Primary Key).
    - `alterid`: Alternative ID for the voucher type.
    - `name`: Child of parent voucher type.
    - `parent`: Parent of the voucher type.
    - `_parent`: Foreign key reference to a parent category.
    - `numbering_method`: Method of numbering for the vouchers.
    - `is_deemedpositive`: Indicates if the voucher type is deemed positive.
    - `affects_stock`: Indicates if the voucher type affects stock.
    - `voucher_reserved_name`: Reserved name for the voucher type [basically it's for Tally].

- `{schema_name}.trn_accounting`
  - **Purpose**: Records accounting transactions.
  - **Columns**:
    - `guid`: Unique identifier for the transaction (Primary Key).
    - `ledger`: Ledger account associated with the transaction.
    - `_ledger`: Foreign key reference to the ledger account.
    - `amount`: Amount involved in the transaction.
    - `amount_forex`: Amount in foreign currency.
    - `currency`: Currency used in the transaction.
    - `bank_allocation_name`: Name of the bank allocation.
    - `bank_date`: Date of the bank transaction.
    - `bank_instrument_date`: Date of the bank instrument.
    - `transaction_type`: Type of transaction.
    - `bank_name`: Name of the bank involved.
    - `instrument_number`: Number of the bank instrument.
    - `unique_ref_number`: Unique reference number for the transaction.
    - `payment_mode`: Mode of payment.
    - `bank_party`: Party involved in the bank transaction.
    - `bank_amount`: Amount in the bank transaction.

- `{schema_name}.mst_ledger`
  - **Purpose**: Maintains ledger accounts.
  - **Columns**:
    - `guid`: Unique identifier for the ledger (Primary Key).
    - `alterid`: Alternative ID for the ledger.
    - `name`: Name of the ledger.
    - `parent`: Parent category of the ledger.
    - `_parent`: Foreign key reference to a parent category.
    - `alias`: Alias for the ledger.
    - `is_revenue`: Indicates if the ledger is a revenue account.
    - `is_deemedpositive`: Indicates if the ledger is deemed positive.
    - `opening_balance`: Opening balance of the ledger.
    - `description`: Description of the ledger.
    - `mailing_name`: Mailing name associated with the ledger.
    - `mailing_address`: Mailing address of the ledger.
    - `mailing_state`: Mailing state.
    - `mailing_country`: Mailing country.
    - `mailing_pincode`: Mailing pincode.
    - `email`: Email address associated with the ledger.
    - `mobile`: Mobile number associated with the ledger.
    - `it_pan`: PAN number for tax purposes.
    - `gstn`: GST number.
    - `gst_registration_type`: GST registration type.
    - `gst_supply_type`: GST supply type.
    - `gst_duty_head`: GST duty head.
    - `tax_rate`: Tax rate applicable.
    - `bank_account_holder`: Name of the bank account holder.
    - `bank_account_number`: Bank account number.
    - `bank_ifsc`: IFSC code of the bank.
    - `bank_swift`: SWIFT code of the bank.
    - `bank_name`: Name of the bank.
    - `bank_branch`: Branch of the bank.
    - `closing_balance`: Closing balance of the ledger.
    - `phone`: Phone number associated with the ledger.
    - `credit_period`: Credit period allowed.
    - `credit_limit`: Credit limit for the ledger.

- `{schema_name}.mst_group`
  - **Purpose**: Defines various groups for categorization.
  - **Columns**:
    - `guid`: Unique identifier for the group (Primary Key).
    - `alterid`: Alternative ID for the group.
    - `name`: Name of the group.
    - `parent`: Parent group.
    - `_parent`: Foreign key reference to a parent group.
    - `primary_group`: Primary group classification.
    - `is_revenue`: Indicates if the group is a revenue group.
    - `is_deemedpositive`: Indicates if the group is deemed positive.
    - `is_reserved`: Indicates if the group is reserved.
    - `affects_gross_profit`: Indicates if the group affects gross profit.
    - `sort_position`: Position for sorting the group.
    - `group_reserved_name`: Reserved name for the group.

- `{schema_name}.mst_uom`
  - **Purpose**: Contains units of measurement for stock items.
  - **Columns**:
    - `guid`: Unique identifier for the unit of measurement (Primary Key).
    - `alterid`: Alternative ID for the unit of measurement.
    - `name`: Name of the unit of measurement.
    - `formalname`: Formal name of the unit.
    - `is_simple_unit`: Indicates if the unit is a simple unit.
    - `base_units`: Base units for conversion.
    - `additional_units`: Additional units for conversion.
    - `conversion`: Conversion factor for the unit.

Relationships:
- `{schema_name}.mst_godown` connects with `{schema_name}.trn_inventory` via `guid` (Primary Key) and `_godown` (Foreign Key).
- `{schema_name}.trn_inventory` connects with `{schema_name}.mst_stock_item` via `_item` (Primary Key) and `guid` (Foreign Key).
- `{schema_name}.trn_voucher` connects with `{schema_name}.trn_inventory` via `guid` (Primary Key) and `guid` (Foreign Key).
- `{schema_name}.trn_bill` connects with `{schema_name}.trn_voucher` via `guid` (Primary Key) and `guid` (Foreign Key).
- `{schema_name}.mst_vouchertype` connects with `{schema_name}.trn_voucher` via `guid` (Primary Key) and `_voucher_type` (Foreign Key).
- `{schema_name}.trn_accounting` connects with `{schema_name}.trn_voucher` via `guid` (Primary Key) and `guid` (Foreign Key).
- `{schema_name}.mst_ledger` connects with `{schema_name}.trn_voucher` via `guid` (Primary Key) and `_party_name` (Foreign Key).
- `{schema_name}.mst_group` connects with `{schema_name}.mst_ledger` via `guid` (Primary Key) and `_parent` (Foreign Key).
- `{schema_name}.mst_stock_item` connects with `{schema_name}.mst_uom` via `_uom` (Primary Key) and `guid` (Foreign Key).



Example 1 - Find out the top suppliers?
The SQL command will be something like this:
WITH TopSupplier AS (
    SELECT v._party_name, v.party_name, ABS(SUM(a.amount)) AS total_amount
    FROM {schema_name}.trn_voucher AS v
    INNER JOIN {schema_name}.trn_accounting AS a ON a.guid = v.guid
    INNER JOIN {schema_name}.mst_vouchertype AS m ON m.guid = v._voucher_type
    INNER JOIN {schema_name}.mst_ledger AS l ON l.guid = v._party_name
    INNER JOIN {schema_name}.mst_group AS g ON g.guid = l._parent
    WHERE v._party_name != a._ledger
    AND m.parent = 'Purchase'
    AND g.primary_group = 'Sundry Creditors'
    AND v.date >= '2023-04-01'
    AND v.date <= '2024-03-31'
    GROUP BY v.party_name, v._party_name
    ORDER BY total_amount DESC
)
SELECT * FROM TopSupplier;

Example 2 - Find out the ITEM SOLD BY QUANTITY?
The SQL command will be something like this:
WITH topItemSoldQuantity AS (
    SELECT A.ITEM, A._ITEM, S.UOM, ABS(SUM(A.AMOUNT)) AS TOTAL_AMOUNT, ABS(SUM(A.QUANTITY)) AS TOTAL_QUANTITY
    FROM {schema_name}.trn_voucher AS V
    INNER JOIN {schema_name}.trn_inventory AS A ON A.guid = V.guid
    INNER JOIN {schema_name}.mst_vouchertype AS M ON M.guid = V._voucher_type
    INNER JOIN {schema_name}.mst_stock_item AS S ON S.guid = A._item
    WHERE M.voucher_reserved_name = 'Sales'
    AND V.date >= '2023-04-01'
    AND V.date <= '2024-03-31'
    GROUP BY A.ITEM, A._ITEM, S.UOM
    ORDER BY TOTAL_QUANTITY DESC
)
SELECT * FROM topItemSoldQuantity;

Example 3 - how many items are there under item group name "Alankar" ?
The SQL command will be something like this:
SELECT count(name) as co 
	FROM ac_a290f6.mst_stock_item
	where parent='Alankar'
	group by parent ;

//TOP ITEM PURCHASE BY VALUE
WITH TopItemPurchase AS
                (SELECT A.ITEM,
                A._ITEM,
                S.UOM,
                ABS(SUM(A.AMOUNT)) AS TOTAL_AMOUNT,
                ABS(SUM(A.QUANTITY)) AS TOTAL_QUANTITY
                FROM  ac_e52fec.TRN_VOUCHER AS V
                INNER JOIN  ac_e52fec.TRN_INVENTORY AS A ON A.GUID = V.GUID
                INNER JOIN  ac_e52fec.MST_VOUCHERTYPE AS M ON M.GUID = V._VOUCHER_TYPE
                INNER JOIN  ac_e52fec.MST_STOCK_ITEM AS S ON S.GUID = A._ITEM
                WHERE M.PARENT = 'Purchase'
                AND V.date >= '2023-04-01'
                AND V.date <= '2024-03-31'
                GROUP BY A.ITEM,A._ITEM,S.UOM
                ORDER BY TOTAL_AMOUNT DESC
SELECT *
FROM TopItemPurchase;

    

Example 4 - Show me the ITEMS PURCHASE BY QUANTITY?
WITH TopItemPurchaseQuantity AS
                (SELECT A.ITEM,
                A._ITEM,
                S.UOM,
                ABS(SUM(A.AMOUNT)) AS TOTAL_AMOUNT,
                ABS(SUM(A.QUANTITY)) AS TOTAL_QUANTITY
                FROM  ac_e52fec.TRN_VOUCHER AS V
                INNER JOIN  ac_e52fec.TRN_INVENTORY AS A ON A.GUID = V.GUID
                INNER JOIN  ac_e52fec.MST_VOUCHERTYPE AS M ON M.GUID = V._VOUCHER_TYPE
                INNER JOIN  ac_e52fec.MST_STOCK_ITEM AS S ON S.GUID = A._ITEM
                WHERE M.PARENT = 'Purchase'
                AND V.date >= '2023-04-01'
                AND V.date <= '2024-03-31'
                GROUP BY A.ITEM,A._ITEM,S.UOM
                ORDER BY TOTAL_QUANTITY DESC;
SELECT *
FROM TopItemPurchaseQuantity;


stictly follow this instruction: The SQL code should not have triple backticks or the word "SQL" in the output. Make sure to use `{schema_name}` before every table name in the query.
. Don't use backticks while writing from statement. Any date should be in "YYYY-MM-DD"
"""

# Streamlit app layout
st.header("🤖AccoBuddy")

# Create columns for input and schema name display
col1, col2 = st.columns([4, 1])

# Input section in the left column
with col1:
    question = st.text_input("Input: ", key="input")
    schema_name = st.text_input("Schema: ", key="schema_input")
    submit = st.button("Ask the question")

# Schema name display on the right
with col2:
    st.write("Selected Schema:")
    st.markdown(
        f"<h2 style='font-size:18px;'>{schema_name}</h2>", unsafe_allow_html=True
    )

if submit:
    response = get_gemini_response(question, prompt, schema_name)
    # Store the database URL in your .env file
    db_url = os.getenv("DATABASE_URL")
    results, colnames, executed_sql = read_sql_query(
        response, db_url, schema_name)
    st.subheader("The SQL Query:")
    st.code(executed_sql, language="sql")

    if (
        isinstance(results, list) and len(results) > 0
    ):  # Successful execution and non-empty result
        st.subheader("The Response is")
        df = pd.DataFrame(results, columns=colnames)  # Create DataFrame
        st.dataframe(df)  # Display the DataFrame as a table
    else:  # If the result is empty or there's an error
        st.subheader(
            "Sorry, but I couldn't understand your question. 😔 Please try searching for a related question. 🔍"
        )
