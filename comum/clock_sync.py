# Implementação baseada no Algoritmo de Cristian

def sync_clock_cristian(local_time, server_time, rtt):
    adjusted_time = server_time + (rtt / 2)
    offset = adjusted_time - local_time
    return offset