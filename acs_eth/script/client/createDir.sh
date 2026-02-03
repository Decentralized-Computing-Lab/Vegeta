#!/bin/bash

NUM=$1

for ((i = 0; i < NUM; i++)); do
{
  name="log/client$i"
  mkdir $name
}
done

wait
