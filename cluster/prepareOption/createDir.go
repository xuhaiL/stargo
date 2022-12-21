package prepareOption

import (
	"fmt"
	"stargo/module"
	"stargo/sr-utl"
)

func CreateDir() {

	var infoMess string

	infoMess = "Create the deploy folder ..."
	utl.Log("OUTPUT", infoMess)
	CreateiSrCtlDir()
	CreateFeDir()
	CreateBeDir()

}

func CreateiSrCtlDir() {

	// create SrCtlDir
	// SRCTLROOT/{download,tmp}

	utl.MkDir(module.GSRCtlRoot + "/tmp")
	utl.MkDir(module.GSRCtlRoot + "/download")
	utl.MkDir(module.GSRCtlRoot + "/cluster")

}

func CreateFeDir() {

	var infoMess string
	var errMess string
	var cmd string
	var err error
	//var outPut []byte

	sshUser := module.GYamlConf.Global.User
	sshKeyRsaFile := module.GSshKeyRsa

	for i := 0; i < len(module.GYamlConf.FeServers); i++ {
		sshHost := module.GYamlConf.FeServers[i].Host
		sshPort := module.GYamlConf.FeServers[i].SshPort

		// create DEPLOY dir for FE nodes
		cmd = fmt.Sprintf("mkdir -p %s", module.GYamlConf.FeServers[i].DeployDir)
		infoMess = fmt.Sprintf("Create DEPLOY Folder for FE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating DEPLOY folder for FE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}

		// create META dir for FE nodes
		cmd = fmt.Sprintf("mkdir -p %s", module.GYamlConf.FeServers[i].MetaDir)
		infoMess = fmt.Sprintf("Create META Folder for FE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating META folder for FE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}

		if module.GYamlConf.FeServers[i].DeployDir+"/meta" != module.GYamlConf.FeServers[i].MetaDir {
			cmd = fmt.Sprintf("ln -s %s %s", module.GYamlConf.FeServers[i].MetaDir, module.GYamlConf.FeServers[i].DeployDir+"/meta")
			infoMess = fmt.Sprintf("Detect MetaDir isn't under DeployDir, Create the soft link, CMD %s", cmd)
			utl.Log("DEBUG", infoMess)
			_, err := utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
			if err != nil {
				errMess = fmt.Sprintf("Error in create soft link for MetaDir, CMD %s", cmd)
				utl.Log("ERROR", errMess)
				panic(err)
			}
		}

		// create LOG dir for FE nodes
		cmd = fmt.Sprintf("mkdir -p %s", module.GYamlConf.FeServers[i].LogDir)
		infoMess = fmt.Sprintf("Create LOG Folder for FE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating LOG folder for FE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}

		if module.GYamlConf.FeServers[i].DeployDir+"/log" != module.GYamlConf.FeServers[i].LogDir {
			cmd = fmt.Sprintf("ln -s %s %s", module.GYamlConf.FeServers[i].LogDir, module.GYamlConf.FeServers[i].DeployDir+"/log")
			infoMess = fmt.Sprintf("Detect LogDir isn't under DeployDir, Create the soft link, CMD %s", cmd)
			utl.Log("DEBUG", infoMess)
			_, err := utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
			if err != nil {
				errMess = fmt.Sprintf("Error in create soft link for LogDir, CMD %s", cmd)
				utl.Log("ERROR", errMess)
				panic(err)
			}
		}
	}
}

func CreateBeDir() {
	var infoMess string
	var errMess string
	var cmd string
	var err error
	//var outPut []byte

	sshUser := module.GYamlConf.Global.User
	sshKeyRsaFile := module.GSshKeyRsa

	for i := 0; i < len(module.GYamlConf.BeServers); i++ {
		sshHost := module.GYamlConf.BeServers[i].Host
		sshPort := module.GYamlConf.BeServers[i].SshPort

		// create DEPLOY dir for BE nodes
		cmd = fmt.Sprintf("mkdir -p %s", module.GYamlConf.BeServers[i].DeployDir)
		infoMess = fmt.Sprintf("Create DEPLOY Folder for BE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating DEPLOY folder for BE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}

		// create STORAGE dir for BE nodes
		cmd = fmt.Sprintf("mkdir -p %s", module.GYamlConf.BeServers[i].StorageDir)
		infoMess = fmt.Sprintf("Create Storage Folder for BE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating STORAGE folder for BE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}

		if module.GYamlConf.BeServers[i].DeployDir+"/storage" != module.GYamlConf.BeServers[i].StorageDir {
			cmd = fmt.Sprintf("ln -s %s %s", module.GYamlConf.BeServers[i].StorageDir, module.GYamlConf.BeServers[i].DeployDir+"/storage")
			infoMess = fmt.Sprintf("Detect StorageDir isn't under DeployDir, Create the soft link, CMD %s", cmd)
			utl.Log("DEBUG", infoMess)
			_, err := utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
			if err != nil {
				errMess = fmt.Sprintf("Error in create soft link for StorageDir, CMD %s", cmd)
				utl.Log("ERROR", errMess)
				panic(err)
			}
		}

		// create LOG dir for BE nodes
		cmd = fmt.Sprintf("mkdir -p %s", module.GYamlConf.BeServers[i].LogDir)
		infoMess = fmt.Sprintf("Create LOG Folder for BE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating LOG folder for BE node: %s@%s:%d \"%s\"", sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}

		if module.GYamlConf.BeServers[i].DeployDir+"/log" != module.GYamlConf.BeServers[i].LogDir {
			cmd = fmt.Sprintf("ln -s %s %s", module.GYamlConf.BeServers[i].LogDir, module.GYamlConf.BeServers[i].DeployDir+"/log")
			infoMess = fmt.Sprintf("Detect BELogDir isn't under DeployDir, Create the soft link, CMD %s", cmd)
			utl.Log("DEBUG", infoMess)
			_, err := utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
			if err != nil {
				errMess = fmt.Sprintf("Error in create soft link for BELogDir, CMD %s", cmd)
				utl.Log("ERROR", errMess)
				panic(err)
			}
		}

	}

}

func CreateAuditPlugins(pluginName string) {
	var infoMess string
	var errMess string
	var cmd string
	var err error
	//var outPut []byte

	sshUser := module.GYamlConf.Global.User
	sshKeyRsaFile := module.GSshKeyRsa

	for i := 0; i < len(module.GYamlConf.FeServers); i++ {
		sshHost := module.GYamlConf.FeServers[i].Host
		sshPort := module.GYamlConf.FeServers[i].SshPort

		// create localPlugins dir for FE nodes
		cmd = fmt.Sprintf("mkdir -p %s/%s/%s", module.GYamlConf.FeServers[i].DeployDir, "localPlugins", pluginName)
		infoMess = fmt.Sprintf("Create localPlugins %s Folder for FE node: %s@%s:%d \"%s\"", pluginName, sshUser, sshHost, sshPort, cmd)
		utl.Log("DEBUG", infoMess)

		_, err = utl.SshRun(sshUser, sshKeyRsaFile, sshHost, sshPort, cmd)
		if err != nil {
			errMess = fmt.Sprintf("ERROR in creating localPlugins %s  folder for FE node: %s@%s:%d \"%s\"", pluginName, sshUser, sshHost, sshPort, cmd)
			utl.Log("ERROR", errMess)
			panic(err)
		}
	}
}
