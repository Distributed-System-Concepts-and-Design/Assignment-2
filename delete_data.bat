@echo off
for /d %%i in (logs_node_*) do @rd /s /q "%%i"
