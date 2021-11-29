# concurrency-control-protocols
Simulation of protocols used in Database Management Systems in order to control concurrency

Currently, there is only one protocol being simulated which is based on Timestamps.
The plan is to keep implementing more protocols frequently.

In order to run the simulation for the Timestamps based protocol you can execute the following command in a terminal from within this repository:

```python timestamp.py log.txt [-twr] ```

The `log.txt` file should list all the operations that you want to simulate in the following order and in separate lines:

```[transaction_name] [operation] [data_item]```
