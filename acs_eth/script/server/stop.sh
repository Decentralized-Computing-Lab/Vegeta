#!/bin/bash

ps -u `whoami` | grep execlayer | awk '{system("kill -9 "$1)}'
ps -u `whoami` | grep ACSclient | awk '{system("kill -9 "$1)}'
ps -u `whoami` | grep ACSserver | awk '{system("kill -9 "$1)}'
