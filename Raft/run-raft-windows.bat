@echo off

title Run Raft Algorithm

set clients=""
set servers=""

set /p clients="Number of clients: "
set /p servers="Number of servers: "

if  %clients% == "" (
    set clients=2
)

if  %servers% == "" (
    set servers=5
)

python raft_setup.py

for /l %%i in (1, 1, %servers%) do (
    start "server %%i" cmd /k "python server.py configs\server-%%i.json"
)

for /l %%j in (1, 1, %clients%) do (
    start "client %%j" cmd /k "python client.py configs\client-%%j.json"
)



