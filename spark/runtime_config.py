import os
import re
import sys
from pathlib import Path


def configure_spark_runtime() -> None:
    if os.name != "nt":
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        return

    java_candidates = []
    configured_java = os.environ.get("JAVA_HOME")
    if configured_java:
        java_candidates.append(Path(configured_java))
    for root in (
        Path("C:/Program Files/Java"),
        Path("C:/Program Files/Eclipse Adoptium"),
    ):
        if root.is_dir():
            java_candidates.extend(root.glob("jdk-*"))

    compatible_java = None
    for candidate in java_candidates:
        release_file = candidate / "release"
        if not release_file.is_file():
            continue
        release_info = release_file.read_text(encoding="utf-8", errors="replace")
        match = re.search(r'^JAVA_VERSION="(?:1\.)?(\d+)', release_info, re.MULTILINE)
        if match and int(match.group(1)) in (17, 21):
            compatible_java = candidate
            break

    if compatible_java is None:
        raise RuntimeError(
            "PySpark 4.0.1 requires JDK 17 or 21 on Windows. "
            "Set JAVA_HOME to an installed JDK 17 or 21."
        )

    os.environ["JAVA_HOME"] = str(compatible_java)
    os.environ["PATH"] = str(compatible_java / "bin") + os.pathsep + os.environ["PATH"]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    hadoop_candidates = []
    configured_hadoop = os.environ.get("HADOOP_HOME")
    if configured_hadoop:
        hadoop_candidates.append(Path(configured_hadoop))
    hadoop_candidates.extend(
        [
            Path.home() / ".hadoop",
            Path("C:/hadoop"),
        ]
    )
    hadoop_home = next(
        (
            candidate
            for candidate in hadoop_candidates
            if (candidate / "bin" / "winutils.exe").is_file()
            and (candidate / "bin" / "hadoop.dll").is_file()
        ),
        None,
    )
    if hadoop_home is None:
        raise RuntimeError(
            "Spark on native Windows requires HADOOP_HOME with "
            "bin\\winutils.exe and bin\\hadoop.dll."
        )

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ["PATH"]
