import logging
import time
from datetime import datetime
from pathlib import Path
from secrets import token_bytes
from typing import List, Optional, Tuple
import os
import json
import sys
from blspy import AugSchemeMPL, G1Element, PrivateKey
from chiapos import DiskPlotter
from chia.plotting.plot_tools import add_plot_directory, stream_plot_info_ph, stream_plot_info_pk
from chia.types.blockchain_format.proof_of_space import ProofOfSpace
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.bech32m import decode_puzzle_hash
from chia.util.keychain import Keychain
from chia.util.path import mkdir
from chia.wallet.derive_keys import master_sk_to_farmer_sk, master_sk_to_local_sk, master_sk_to_pool_sk
import threading
log = logging.getLogger(__name__)


def get_farmer_public_key(alt_fingerprint: Optional[int] = None) -> G1Element:
    sk_ent: Optional[Tuple[PrivateKey, bytes]]
    keychain: Keychain = Keychain()
    if alt_fingerprint is not None:
        sk_ent = keychain.get_private_key_by_fingerprint(alt_fingerprint)
    else:
        sk_ent = keychain.get_first_private_key()
    if sk_ent is None:
        raise RuntimeError(
            "No keys, please run 'chia keys add', 'chia keys generate' or provide a public key with -f")
    return master_sk_to_farmer_sk(sk_ent[0]).get_g1()


def get_pool_public_key(alt_fingerprint: Optional[int] = None) -> G1Element:
    sk_ent: Optional[Tuple[PrivateKey, bytes]]
    keychain: Keychain = Keychain()
    if alt_fingerprint is not None:
        sk_ent = keychain.get_private_key_by_fingerprint(alt_fingerprint)
    else:
        sk_ent = keychain.get_first_private_key()
    if sk_ent is None:
        raise RuntimeError(
            "No keys, please run 'chia keys add', 'chia keys generate' or provide a public key with -p")
    return master_sk_to_pool_sk(sk_ent[0]).get_g1()


def create_plots(
    farmer_public_key,
    pool_public_key,
    tmp_dir,
    k,
    queue,
    buckets=128,
    nobitfield=False,
    num_threads=2,
    buffer=3389,
    root_path='/root/.chia/mainnet',
    use_datetime=True,
    test_private_keys: Optional[List] = None,
    log_path='./',
    posters=[],
    task_id="",
    pid=0,
    meta_info=""
):

    logdir = os.path.join(log_path, str(task_id)+str(int(time.time())))
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    with open(os.path.join(logdir, 'stdout.log'), 'w') as outfd, open(os.path.join(logdir, 'stderr.log'), 'w') as errfd:
        sys.stdout = outfd
        sys.stderr = errfd
        stdout_fileno = sys.stdout.fileno()
        stdout_save = os.dup(stdout_fileno)
        stdout_pipe = os.pipe()
        os.dup2(stdout_pipe[1], stdout_fileno)
        os.close(stdout_pipe[1])
        def drain_pipe(CRLF='\n'):
            global captured_stdout
            while True:
                data = os.read(stdout_pipe[0], 32)
                if not data:
                    break
                outfd.write(data.decode()+CRLF)

        t = threading.Thread(target=drain_pipe)
        t.start()
        tmp2_dir = tmp_dir

        farmer_public_key = G1Element.from_bytes(
            bytes.fromhex(farmer_public_key))
        pool_public_key = G1Element.from_bytes(bytes.fromhex(pool_public_key))

        num = 1
        size = k

        if pool_public_key is not None:
            log.info(
                f"Creating {num} plots of size {size}, pool public key:  "
                f"{bytes(pool_public_key).hex()} farmer public key: {bytes(farmer_public_key).hex()}"
            )

        tmp_dir_created = False
        if not os.path.exists(tmp_dir):
            mkdir(tmp_dir)
            tmp_dir_created = True

        tmp2_dir_created = False
        if not os.path.exists(tmp2_dir):
            mkdir(tmp2_dir)
            tmp2_dir_created = True

        finished_filenames = []
        for i in range(num):
            # Generate a random master secret key
            if test_private_keys is not None:
                assert len(test_private_keys) == num
                sk: PrivateKey = test_private_keys[i]
            else:
                sk = AugSchemeMPL.key_gen(token_bytes(32))

            # The plot public key is the combination of the harvester and farmer keys
            plot_public_key = ProofOfSpace.generate_plot_public_key(
                master_sk_to_local_sk(sk).get_g1(), farmer_public_key)

            # The plot id is based on the harvester, farmer, and pool keys
            if pool_public_key is not None:
                plot_id: bytes32 = ProofOfSpace.calculate_plot_id_pk(
                    pool_public_key, plot_public_key)
                plot_memo: bytes32 = stream_plot_info_pk(
                    pool_public_key, farmer_public_key, sk)

            # Uncomment next two lines if memo is needed for dev debug
            plot_memo_str: str = plot_memo.hex()
            log.info(f"Memo: {plot_memo_str}")

            dt_string = datetime.now().strftime("%Y-%m-%d-%H-%M")

            if use_datetime:
                filename: str = f"plot-k{size}-{dt_string}-{plot_id}.plot"
            else:
                filename = f"plot-k{size}-{plot_id}.plot"
            full_path = os.path.join(tmp_dir, filename)

            if not os.path.exists(full_path):
                log.info(f"Starting plot {i + 1}/{num}")
                # Creates the plot. This will take a long time for larger plots.
                plotter: DiskPlotter = DiskPlotter()
                plotter.create_plot_disk(
                    str(tmp_dir),
                    str(tmp2_dir),
                    str(tmp_dir),
                    filename,
                    size,
                    plot_memo,
                    plot_id,
                    buffer,
                    buckets,
                    65536,
                    num_threads,
                    nobitfield,
                )
                finished_filenames.append(filename)
            else:
                log.info(f"Plot {filename} already exists")
        queue.put(json.dumps({
            "dir": tmp_dir,
            "filename": finished_filenames[0],
            "pid": pid,
            "task_id": task_id,
            "posters": posters,
            "child_pid": os.getpid(),
            "meta_info": meta_info
        }))
        t.join()
        os.close(stdout_fileno)
