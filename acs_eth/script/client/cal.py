import sys

nodes = int(sys.argv[1])
bathsize = int(sys.argv[2])
path = str(sys.argv[3])
filen = str(sys.argv[4])
duration = int(sys.argv[5])
latency = []
tx = 0

for i in range(nodes):
    filename = path + str(i) + '/' + filen
    latency_one = []
    with open(filename,"r") as f:
        lines = f.readlines()
        tx += len(lines)
        for line in lines:
            try:
                if(sum(latency_one) <= duration * 1000):
                    data = float(line)
                    latency.append(data)
                    latency_one.append(data)
                else:
                    break
            except:
                break

print("Total Time (ms): ", duration)
print("Throughput (tx/s): ", tx * bathsize / duration)
print("Latency (ms): ", sum(latency)/len(latency))
