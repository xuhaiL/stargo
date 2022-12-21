package plugins

import (
	"errors"
	"fmt"
	"github.com/howeyc/gopass"
	"os"
	"stargo/cluster/checkStatus"
	"stargo/cluster/prepareOption"
	"stargo/module"
	"stargo/sr-utl"
)

var pluginName string

func DeployAudit(clusterName string) {
	initAuditModule(clusterName)

	// 0. 执行建库语句，建表语句;
	// 1. 创建对应db的用户权限 audit_u/starrocks_audit_tbl__;
	// create user 'audit_u' identified by 'starrocks_audit_tbl__p';
	// grant SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV on starrocks_audit_db__.starrocks_audit_tbl__ to audit_u;
	// 2. 分发插件，分发到所有FE节点中
	// 3. 执行插件安装语句
	// install plugin from '/volume/plugins/auditloader/';

	sqlIp := module.GFeEntryHost
	sqlPort := module.GFeEntryQueryPort
	sqlUserName := "root"
	sqlPassword := module.GJdbcPasswd
	sqlDbName := ""

	initUserSql := fmt.Sprintf("create user if not exists 'audit_u' identified by 'starrocks_audit_tbl__p'")
	_, initUserSqlErr := utl.RunSQL(sqlUserName, sqlPassword, sqlIp, sqlPort, sqlDbName, initUserSql)
	checkError(initUserSql, initUserSqlErr)

	//grantUserSql := "grant SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV on starrocks_audit_db__.starrocks_audit_tbl__ to audit_u"
	grantUserSql := "grant SELECT_PRIV, LOAD_PRIV on starrocks_audit_db__.starrocks_audit_tbl__ to audit_u"
	_, grantUserSqlErr := utl.RunSQL(sqlUserName, sqlPassword, sqlIp, sqlPort, sqlDbName, grantUserSql)
	checkError(grantUserSql, grantUserSqlErr)

	_, createDatabaseSqlErr := utl.RunSQL(sqlUserName, sqlPassword, sqlIp, sqlPort, sqlDbName, createDatabaseSql)
	checkError(createDatabaseSql, createDatabaseSqlErr)

	var replicationNum = 1
	if len(module.GYamlConf.BeServers) >= 3 {
		replicationNum = 3
	}

	formatCTS := fmt.Sprintf(createTableSql, replicationNum)
	_, createTableSqlErr := utl.RunSQL(sqlUserName, sqlPassword, sqlIp, sqlPort, sqlDbName, formatCTS)
	checkError(formatCTS, createTableSqlErr)

	prepareOption.CreateAuditPlugins(pluginName)
	prepareOption.DistributeAuditDir(pluginName)

	// 多FE 节点 deploy 路径一致
	installPlugin := fmt.Sprintf("install plugin from '%s/%s/%s/auditloader.zip'", module.GYamlConf.FeServers[module.GFEEntryId].DeployDir, "localPlugins", pluginName)
	_, installPluginSqlErr := utl.RunSQL(sqlUserName, sqlPassword, sqlIp, sqlPort, sqlDbName, installPlugin)
	checkError(installPlugin, installPluginSqlErr)

}

func UninstallAudit(clusterName string) {
	initAuditModule(clusterName)
	sqlIp := module.GFeEntryHost
	sqlPort := module.GFeEntryQueryPort
	sqlUserName := "root"
	sqlPassword := module.GJdbcPasswd
	sqlDbName := ""
	unInstallPlugin := fmt.Sprintf("UNINSTALL PLUGIN AuditLoader")
	_, unInstallPluginSqlErr := utl.RunSQL(sqlUserName, sqlPassword, sqlIp, sqlPort, sqlDbName, unInstallPlugin)
	checkError(unInstallPlugin, unInstallPluginSqlErr)
	utl.Log("INFO", "uninstall plugin AuditLoader success")
}

func checkError(sql string, err error) {
	if err != nil {
		utl.Log("ERROR", fmt.Sprintf("use sql %s error, error mesage %s", sql, err))
		os.Exit(1)
	}
}

func initAuditModule(clusterName string) {
	pluginName = "audit"
	var infoMess string
	module.InitConf(clusterName, "")
	initFEEntry()

	if checkStatus.CheckClusterName(clusterName) {
		infoMess = "Don't find the Cluster " + clusterName
		utl.Log("ERROR", infoMess)
		os.Exit(1)
	}

	module.SetGlobalVar("GSRVersion", module.GYamlConf.ClusterInfo.Version)
	module.GetRepo()

	fmt.Println("Please set StarRocks root PassWord")
	passwd, _ := gopass.GetPasswd()
	module.GJdbcPasswd = string(passwd)
}

func initFEEntry() {
	var infoMess string
	// get FE entry
	feEntryId, err := checkStatus.GetFeEntry(-1)
	//tmpFeEntryHost = yamlConf.FeServers[feEntryId].Host
	//tmpFeEntryPort = yamlConf.FeServers[feEntryId].QueryPort
	module.SetFeEntry(feEntryId)
	if err != nil || feEntryId == -1 {
		infoMess = "Error in get the FE entry, pls check FE status."
		utl.Log("ERROR", infoMess)
		err = errors.New(infoMess)
		panic(err)
	}
}
