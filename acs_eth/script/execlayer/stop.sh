#!/bin/bash

ps -u `whoami` | grep execlayer | awk '{system("kill -9 "$1)}'
