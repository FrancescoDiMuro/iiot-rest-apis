rem start "IIoT Collector" /B /wait .\env\Scripts\activate
start "IIoT Collector" /B .\env\Scripts\python.exe collector\client.py
rem start .\env\Scripts\python.exe rest-apis\create_chart.py