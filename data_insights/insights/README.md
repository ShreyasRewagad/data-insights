Input CSVs structure:

 - invoice

 	- invoice_id - Invoice UUID,
	- invoice_date - Issue date,
	- due_date - Due date,
	- period_start_date - Start date (if any) of service dates that invoice covers,
	- period_end_date - End date (if any) of service dates that invoice covers,
	- total_amount - Billed Amount,
	- canonical_vendor_id - Vendor UUID

 - line_item

 	- invoice_id - Invoice UUID,
	- line_item_id - Raw Line Item UUID,
	- period_start_date - Start date (if any) of service dates that raw line item covers,
	- period_end_date - End date (if any) of service dates that raw line item covers,
	- total_amount - Billed Amount,
	- canonical_line_item_id - Canonical line item UUID

Additional notes:
	
- 1:M relationship between invoice and line_item
- 1:M relationship between line_item_id and canonical_line_item_id (i.e. different raw line items can be mapped to a single canonical line item)

Gleans (insights) to be generated and backfilled in the past:

 - `VendorNotSeenInAWhile`

	- logic:
	    1) don't trigger if invoice received from the vendor for the first time
	    2) trigger if invoice received from the vendor and it's > 90 days since last `invoice_date`

	- `glean_text` (text of the glean to be created):
		First new bill in [count_of_months_since_last_invoice] months from vendor [canonical_vendor_id]

	- `glean_location`:
		invoice (this glean should be created on an invoice level)


 - `AccrualAlert`

	- logic:
	    1) trigger if `period_end_date` for invoice or any line item > 90 days after `invoice_date`. If there are multiple end dates, pick the last one.

	- `glean_text` (text of the glean to be created):
		Line items from vendor [canonical_vendor_id] in this invoice cover future periods (through [period_end_date])

	- `glean_location`:
		invoice (this glean should be created on an invoice level)


 - LargeMonthIncreaseMtd

	- logic:
	    1) trigger if monthly spend > $10K and it increased > 50% of average spend over last 12 months. If monthly spend is less than $10K, > 200%. If less than $1K, > 500%. If less than $100, don't trigger the glean. Spend is sum of invoice `total_amount`.
    
	- `glean_text` (text of the glean to be created):
		Monthly spend with [canonical_vendor_id] is $x (x%) higher than average

	- `glean_location`:
		vendor (this glean should be created on a vendor level)


 - NoInvoiceReceived

	- logic:
	    1) trigger if vendor sends invoice(s) either on MONTHLY basis (1 or more per month) or QUARTERLY basis (1 per quarter).

		    1.1) MONTHLY case: trigger if there were 3 consecutive months with invoices received from vendor but there are no invoices received in current month.
			 Start triggering the glean from the day when vendor usually sends the invoice (you need to count day frequency). Keep triggering the glean till the end of the current month or until the day when new invoice received.
			 If there are multiple days based on frequency count, pick the earliest one.

		    1.2) QUARTERLY case: trigger if there were 2 consecutive quarters with invoices received from vendor but there are no invoices received in current quarter.
			 Start triggering the glean from the day when vendor usually sends the invoice (you need to count day frequency). Keep triggering the glean till the end of the current month of the quarter or until the day when new invoice received.
			 If there are multiple days based on frequency count, pick the earliest one.

	- `glean_text` (text of the glean to be created):
		[canonical_vendor_id] generally charges between on [most_frequent_day_number] day of each month invoices are sent. On [date], an invoice from [canonical_vendor_id] has not been received

	- `glean_location`:
		vendor (this glean should be created on a vendor level)

Output CSV structure:

- `glean_id` - Glean UUID
- `glean_date` - Date when glean was triggered
- `glean_text` - Glean Text
- `glean_type` - Enum (vendor_not_seen_in_a_while, accrual_alert, large_month_increase_mtd, no_invoice_received)
- `glean_location` - Enum (invoice or vendor)
- `invoice_id` - Invoice UUID
- `canonical_vendor_id` - Vendor UUID