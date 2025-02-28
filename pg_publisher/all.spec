# -*- mode: python ; coding: utf-8 -*-

import shutil
shutil.copyfile('pg_publisher/conf.ini', '{0}/conf.ini'.format(DISTPATH))

block_cipher = None

added_files = [
    ( 'pg_publisher/conf.ini', '.' )]

cli_a = Analysis(
    ['pg_publisher/cli.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['configparser'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

cli_direct_a = Analysis(
    ['pg_publisher/cli_direct.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['configparser'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# MERGE( (cli_a, 'cli', 'cli'), (cli_direct_a, 'cli_direct', 'cli_direct') )

cli_pyz = PYZ(cli_a.pure, cli_a.zipped_data, cipher=block_cipher)

cli_direct_pyz = PYZ(cli_direct_a.pure, cli_direct_a.zipped_data, cipher=block_cipher)

cli_exe = EXE(
    cli_pyz,
    cli_a.scripts,
    cli_a.binaries,
    cli_a.zipfiles,
    cli_a.datas,
    [],
    name='cli',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

cli_direct_exe = EXE(
    cli_direct_pyz,
    cli_direct_a.scripts,
    cli_direct_a.binaries,
    cli_direct_a.zipfiles,
    cli_direct_a.datas,
    [],
    name='cli_direct',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
