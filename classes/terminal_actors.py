import subprocess
from timeit import default_timer as timer


class TerminalActor:

    @staticmethod
    def run_shell_cmd(cmd: str, print_stdout: bool = False):
        print(f'Executing shell command:\n{cmd}')
        start_time = timer()
        res = subprocess.run(cmd, shell=True, capture_output=True)
        if print_stdout:
            print(res.stdout)
        print(f'Shell command execution duration: {timer() - start_time:.4f} sec.')
        return res
