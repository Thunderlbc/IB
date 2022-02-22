from utils.common import run_cmd_with_stdout_return, run_cmd, time_out
from utils.config import Config
from chia.plotting.create_plots import get_farmer_public_key, get_pool_public_key
import re
import subprocess


@time_out(Config.timeout.generate_account)
def generate_new_chia_account():
    cmd = ['chia', 'keys', 'generate']
    result = run_cmd_with_stdout_return(cmd)
    pattern = 'Generating private key\n\nAdded private key with public key fingerprint (.*) and mnemonic\n\n(.*)\n'
    res = re.search(pattern, result)
    fingerprint = int(res.group(1))

    p = subprocess.Popen(['chia-plotter-linux-amd64'],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=False)
    tmp = res.group(2) + '\n'
    p.stdin.write(tmp.encode())
    p.stdin.close()
    hpool_res = p.stdout.read().decode()
    p.terminate()

    pattern = 'level=info msg="Fingerprint: (.*)"\ntime=".*" level=info msg="Farmer Public Key \(fpk\): .*"\ntime=".*" level=info msg="Pool Public Key \(ppk\): .*"\ntime=".*" level=info msg="Signature: (.*)"\ntime=".*" level=info msg="Signature for address: .*"\ntime=".*" level=info msg="Signature Expiry at: (.*) '
    hres = re.search(pattern, hpool_res)
    hfingerprint, hsignature, hexpiry = hres.group(
        1), hres.group(2), hres.group(3)

    assert(fingerprint == int(hfingerprint))

    return {
        "fingerprint": fingerprint,
        "mnemonic": res.group(2),
        "farmer_pkey": get_farmer_public_key(fingerprint),
        "pool_pkey": get_pool_public_key(fingerprint),
        "hsignature": hsignature,
        "hexpiry": hexpiry
    }


if __name__ == "__main__":
    print(generate_new_chia_account())
