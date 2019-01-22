This repository has scripts that enable serializing a CSV file that contains columns for site IDs, datetime, and n variables that is described by and Agave Metadata "Timeseries" document.  The rows are serialized to Agave Metadata "Observation" documents that contain associations with the corresponding Metadata "Site" and "Variable" documents referenced in the "Timeseries".

To run use:
<pre>
python3 parse_spreadsheet_timeseries.py -e 'agaveauth.its.hawaii.edu' -t '1111ef346b127987f1918db488f1111' -i test.csv --uuid='9111709891106968041-242ac1111-0001-111'
</pre>

This assumes you have a valid Agave Auth Token and valid "Timeseries" document uuid stored within the Agave endpoint.
