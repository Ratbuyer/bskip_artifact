import subprocess

start_size = 6
end_size = 9
for i in range(start_size, end_size + 1):
  n = 10**i
  subprocess.call(['numactl', '-i', 'all', './basic', str(n)])
