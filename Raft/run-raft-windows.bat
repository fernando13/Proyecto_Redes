@echo off

title Run Raft Algorithm

set qty_servers=""
set servers=""
set clients=""

set /p qty_servers="Number of servers in the whole system: "
set /p servers="Number of servers to execute: "
set /p clients="Number of clients to execute: "

if  %qty_servers% == "" (
    set qty_servers=5
)

if  %servers% == "" (
    set servers=5
)

if  %clients% == "" (
    set clients=2
)


python raft_setup.py %qty_servers% %clients%

for /l %%i in (1, 1, %servers%) do (
    start "server %%i" cmd /k "python server.py configs\server-%%i.json"
)

for /l %%j in (1, 1, %clients%) do (
    start "client %%j" cmd /k "python client.py configs\client-%%j.json"
)



