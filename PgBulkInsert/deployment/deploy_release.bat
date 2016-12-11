:: Copyright (c) Philipp Wagner. All rights reserved.
:: Licensed under the MIT license. See LICENSE file in the project root for full license information.

@echo off

echo ---------------------------------------------------
echo - Bundling Artifacts for OSSRH Repository Upload  -
echo ---------------------------------------------------

:: Define the Executables, so we don't have to rely on pathes:
set MVN_EXECUTABLE="C:\Program Files (x86)\Maven\apache-maven-3.3.9\bin\mvn.cmd"
set GPG_EXECUTABLE="C:\Program Files (x86)\GNU\GnuPG\pub\gpg.exe"

:: GPG Key ID used for signing:
set GPG_KEY_ID=E4B54CD3

:: Logs to be used:
set STDOUT=stdout.log
set STDERR=stderr.log

:: POM File to use for building the project:
set POM_FILE=..\pom.xml

:: Prompt for Sonatype:
set /p SONATYPE_USER="Sonatype User: "
set /p SONATYPE_PASSWORD="Sonatype Password: "

:: Prompt GPG Passphrase:
set /p GPG_PASSPHRASE="GPG Signing Passphrase: "

1>%STDOUT% 2>%STDERR% (

    %MVN_EXECUTABLE% clean deploy -Prelease,docs-and-source --settings deploysettings.xml -DskipTests -Dgpg.keyname=%GPG_KEY_ID% -Dgpg.executable=%GPG_EXECUTABLE% -Dgpg.passphrase=%GPG_PASSPHRASE% -DretryFailedDeploymentCount=3 -f %POM_FILE%
  
)

pause
