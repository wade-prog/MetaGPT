import asyncio
import os
import re
import shutil
from asyncio import Queue
from asyncio.subprocess import PIPE, STDOUT
from typing import Optional

from metagpt.config2 import Config
from metagpt.const import DEFAULT_WORKSPACE_ROOT, SWE_SETUP_PATH
from metagpt.logs import logger
from metagpt.tools.tool_registry import register_tool
from metagpt.utils.report import END_MARKER_VALUE, TerminalReporter


@register_tool()
class Terminal:
    """
    A tool for running terminal commands.
    Don't initialize a new instance of this class if one already exists.
    For commands that need to be executed within a Conda environment, it is recommended
    to use the `execute_in_conda_env` method.
    """

    def __init__(self, shell: Optional[str] = None):
        self._shell_preference = (shell or os.getenv("METAGPT_SHELL") or "").strip().lower()
        self.shell_command: list[str] = []
        self.executable: Optional[str] = None
        self.shell_flavor: str = ""
        self.command_terminator = "\n"
        self._pwd_command = "pwd"
        # End marker must be safe for the chosen shell.
        # NOTE: END_MARKER_VALUE contains a newline (and control chars). That works in bash but breaks cmd.exe.
        self._end_marker_value = END_MARKER_VALUE
        self.stdout_queue = Queue(maxsize=1000)
        self.observer = TerminalReporter()
        self.process: Optional[asyncio.subprocess.Process] = None
        #  The cmd in forbidden_terminal_commands will be replace by pass ana return the advise. example:{"cmd":"forbidden_reason/advice"}
        self.forbidden_commands = {
            "run dev": "Use Deployer.deploy_to_public instead.",
            # serve cmd have a space behind it,
            "serve ": "Use Deployer.deploy_to_public instead.",
        }

        self._configure_shell()

    def _configure_shell(self) -> None:
        """Choose an interactive shell implementation per OS.

        - Prefer bash on Unix.
        - On Windows, prefer bash if available (Git Bash/WSL), otherwise fall back to cmd.exe.

        Users can override with env var `METAGPT_SHELL` or constructor arg `shell`.
        Supported values: bash, cmd.
        """

        def _which_any(*names: str) -> Optional[str]:
            for name in names:
                path = shutil.which(name)
                if path:
                    return path
            return None

        if os.name != "nt":
            bash = _which_any("bash", "sh")
            self.executable = bash or "bash"
            self.shell_command = [self.executable]
            self.shell_flavor = "bash"
            self.command_terminator = "\n"
            self._pwd_command = "pwd"
            self._end_marker_value = END_MARKER_VALUE
            return

        # Windows
        pref = self._shell_preference
        bash = _which_any("bash")

        if pref in {"bash", "sh"}:
            if not bash:
                raise FileNotFoundError(
                    "METAGPT_SHELL=bash requested, but 'bash' was not found. "
                    "Install Git for Windows (Git Bash) or enable WSL, or set METAGPT_SHELL=cmd."
                )
            self.executable = bash
            self.shell_command = [bash]
            self.shell_flavor = "bash"
            self.command_terminator = "\n"
            self._pwd_command = "pwd"
            self._end_marker_value = END_MARKER_VALUE
            return

        if pref in {"cmd", "cmd.exe"}:
            self.executable = _which_any("cmd.exe") or "cmd.exe"
            # /Q: turn echo off, /K: keep the session running
            self.shell_command = [self.executable, "/Q", "/K"]
            self.shell_flavor = "cmd"
            self.command_terminator = "\r\n"
            self._pwd_command = "cd"
            # cmd.exe cannot reliably echo control chars/newlines used by END_MARKER_VALUE.
            self._end_marker_value = "__METAGPT_CMD_END__"
            return

        # Auto on Windows: prefer bash if available, else cmd.
        if bash:
            self.executable = bash
            self.shell_command = [bash]
            self.shell_flavor = "bash"
            self.command_terminator = "\n"
            self._pwd_command = "pwd"
            self._end_marker_value = END_MARKER_VALUE
        else:
            self.executable = _which_any("cmd.exe") or "cmd.exe"
            self.shell_command = [self.executable, "/Q", "/K"]
            self.shell_flavor = "cmd"
            self.command_terminator = "\r\n"
            self._pwd_command = "cd"
            self._end_marker_value = "__METAGPT_CMD_END__"

    def _format_end_marker_command(self) -> str:
        # In cmd.exe, the marker must not contain newline/control chars.
        # Also, `echo "MARK"` prints quotes too. Use no quotes.
        if self.shell_flavor == "cmd":
            return f"echo {self._end_marker_value}"

        # bash/sh: keep the existing marker (contains newline+control chars) to preserve behavior.
        return f'echo "{self._end_marker_value}"'

    def _compat_command(self, cmd: str) -> str:
        """Best-effort compatibility for Windows cmd.exe when the agent emits POSIX-ish commands."""
        if os.name != "nt" or self.shell_flavor != "cmd":
            return cmd

        # Common POSIX -> cmd translations.
        cmd = re.sub(r"(?<!\S)pwd(?!\S)", "cd", cmd)
        cmd = re.sub(r"(?<!\S)mkdir\s+-p\s+", "mkdir ", cmd)
        return cmd

    async def _start_process(self):
        # Start a persistent shell process
        self.process = await asyncio.create_subprocess_exec(
            *self.shell_command,
            stdin=PIPE,
            stdout=PIPE,
            stderr=STDOUT,
            executable=self.executable,
            env=os.environ.copy(),
            cwd=DEFAULT_WORKSPACE_ROOT.absolute(),
        )
        await self._check_state()

    async def _check_state(self):
        """
        Check the state of the terminal, e.g. the current directory of the terminal process. Useful for agent to understand.
        """
        output = await self.run_command(self._pwd_command)
        logger.info("The terminal is at:", output)

    async def run_command(self, cmd: str, daemon=False) -> str:
        """
        Executes a specified command in the terminal and streams the output back in real time.
        This command maintains state across executions, such as the current directory,
        allowing for sequential commands to be contextually aware.

        Args:
            cmd (str): The command to execute in the terminal.
            daemon (bool): If True, executes the command in an asynchronous task, allowing
                           the main program to continue execution.
        Returns:
            str: The command's output or an empty string if `daemon` is True. Remember that
                 when `daemon` is True, use the `get_stdout_output` method to get the output.
        """
        if self.process is None:
            await self._start_process()

        output = ""

        cmd = self._compat_command(cmd)

        # Remove forbidden commands
        commands = re.split(r"\s*&&\s*", cmd)
        for cmd_name, reason in self.forbidden_commands.items():
            # "true" is a pass command in linux terminal.
            for index, command in enumerate(commands):
                if cmd_name in command:
                    output += f"Failed to execut {command}. {reason}\n"
                    commands[index] = "true"
        cmd = " && ".join(commands)

        try:
            # Send the command
            self.process.stdin.write((cmd + self.command_terminator).encode())
            self.process.stdin.write(
                f"{self._format_end_marker_command()}{self.command_terminator}".encode()  # write EOF
            )  # Unique marker to signal command end
            await self.process.stdin.drain()
            if daemon:
                asyncio.create_task(self._read_and_process_output(cmd))
            else:
                output += await self._read_and_process_output(cmd)
        except asyncio.CancelledError:
            # Best-effort cleanup so Ctrl+C doesn't leave subprocess transports dangling.
            try:
                await self.close()
            finally:
                raise

        return output

    async def execute_in_conda_env(self, cmd: str, env, daemon=False) -> str:
        """
        Executes a given command within a specified Conda environment automatically without
        the need for manual activation. Users just need to provide the name of the Conda
        environment and the command to execute.

        Args:
            cmd (str): The command to execute within the Conda environment.
            env (str, optional): The name of the Conda environment to activate before executing the command.
                                 If not specified, the command will run in the current active environment.
            daemon (bool): If True, the command is run in an asynchronous task, similar to `run_command`,
                           affecting error logging and handling in the same manner.

        Returns:
            str: The command's output, or an empty string if `daemon` is True, with output processed
                 asynchronously in that case.

        Note:
            This function wraps `run_command`, prepending the necessary Conda activation commands
            to ensure the specified environment is active for the command's execution.
        """
        cmd = f"conda run -n {env} {cmd}"
        return await self.run_command(cmd, daemon=daemon)

    async def get_stdout_output(self) -> str:
        """
        Retrieves all collected output from background running commands and returns it as a string.

        Returns:
            str: The collected output from background running commands, returned as a string.
        """
        output_lines = []
        while not self.stdout_queue.empty():
            line = await self.stdout_queue.get()
            output_lines.append(line)
        return "\n".join(output_lines)

    async def _read_and_process_output(self, cmd, daemon=False) -> str:
        async with self.observer as observer:
            cmd_output = []
            await observer.async_report(cmd + self.command_terminator, "cmd")
            # report the command
            # Read the output until the unique marker is found.
            # We read bytes directly from stdout instead of text because when reading text,
            # '\r' is changed to '\n', resulting in excessive output.
            tmp = b""
            while True:
                chunk = await self.process.stdout.read(1)
                if chunk == b"":
                    # EOF: subprocess ended unexpectedly; avoid infinite loop.
                    raise RuntimeError("Terminal subprocess ended unexpectedly (EOF).")

                output = tmp + chunk
                *lines, tmp = output.splitlines(True)
                for line in lines:
                    # Use a tolerant decode because Windows cmd output may not be UTF-8.
                    line = line.decode(errors="replace")
                    ix = line.rfind(self._end_marker_value)
                    if ix >= 0:
                        line = line[0:ix]
                        if line:
                            await observer.async_report(line, "output")
                            # report stdout in real-time
                            cmd_output.append(line)
                        return "".join(cmd_output)
                    # log stdout in real-time
                    await observer.async_report(line, "output")
                    cmd_output.append(line)
                    if daemon:
                        await self.stdout_queue.put(line)

    async def close(self):
        """Close the persistent shell process."""
        self.process.stdin.close()
        await self.process.wait()


@register_tool(include_functions=["run"])
class Bash(Terminal):
    """
    A class to run bash commands directly and provides custom shell functions.
    All custom functions in this class can ONLY be called via the `Bash.run` method.
    """

    def __init__(self):
        """init"""
        os.environ["SWE_CMD_WORK_DIR"] = str(Config.default().workspace.path)
        super().__init__(shell="bash")
        self.start_flag = False

    async def start(self):
        await self.run_command(f"cd {Config.default().workspace.path}")
        await self.run_command(f"source {SWE_SETUP_PATH}")

    async def run(self, cmd) -> str:
        """
        Executes a bash command.

        Args:
            cmd (str): The bash command to execute.

        Returns:
            str: The output of the command.

        This method allows for executing standard bash commands as well as
        utilizing several custom shell functions defined in the environment.

        Custom Shell Functions:

        - open <path> [<line_number>]
          Opens the file at the given path in the editor. If line_number is provided,
          the window will move to include that line.
          Arguments:
              path (str): The path to the file to open.
              line_number (int, optional): The line number to move the window to.
              If not provided, the window will start at the top of the file.

        - goto <line_number>
          Moves the window to show <line_number>.
          Arguments:
              line_number (int): The line number to move the window to.

        - scroll_down
          Moves the window down {WINDOW} lines.

        - scroll_up
          Moves the window up {WINDOW} lines.

        - create <filename>
          Creates and opens a new file with the given name.
          Arguments:
              filename (str): The name of the file to create.

        - search_dir_and_preview <search_term> [<dir>]
          Searches for search_term in all files in dir and gives their code preview
          with line numbers. If dir is not provided, searches in the current directory.
          Arguments:
              search_term (str): The term to search for.
              dir (str, optional): The directory to search in. Defaults to the current directory.

        - search_file <search_term> [<file>]
          Searches for search_term in file. If file is not provided, searches in the current open file.
          Arguments:
              search_term (str): The term to search for.
              file (str, optional): The file to search in. Defaults to the current open file.

        - find_file <file_name> [<dir>]
          Finds all files with the given name in dir. If dir is not provided, searches in the current directory.
          Arguments:
              file_name (str): The name of the file to search for.
              dir (str, optional): The directory to search in. Defaults to the current directory.

        - edit <start_line>:<end_line> <<EOF
          <replacement_text>
          EOF
          Line numbers start from 1. Replaces lines <start_line> through <end_line> (inclusive) with the given text in the open file.
          The replacement text is terminated by a line with only EOF on it. All of the <replacement text> will be entered, so make
          sure your indentation is formatted properly. Python files will be checked for syntax errors after the edit. If the system
          detects a syntax error, the edit will not be executed. Simply try to edit the file again, but make sure to read the error
          message and modify the edit command you issue accordingly. Issuing the same command a second time will just lead to the same
          error message again. All code modifications made via the 'edit' command must strictly follow the PEP8 standard.
          Arguments:
              start_line (int): The line number to start the edit at, starting from 1.
              end_line (int): The line number to end the edit at (inclusive), starting from 1.
              replacement_text (str): The text to replace the current selection with, must conform to PEP8 standards.

        - submit
          Submits your current code locally. it can only be executed once, the last action before the `end`.

        Note: Make sure to use these functions as per their defined arguments and behaviors.
        """
        if not self.start_flag:
            await self.start()
            self.start_flag = True

        return await self.run_command(cmd)
