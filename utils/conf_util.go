package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/go-courier/envconf"
	"github.com/go-courier/reflectx"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var config = &Configuration{}

func init() {
	config.init()
}

type ConfigurationOption = func(conf *Configuration)

func Set(configurationOptions ...ConfigurationOption) {
	options := append(
		[]ConfigurationOption{
			WithOutputDir("./"),
		},
		configurationOptions...,
	)

	for i := range options {
		options[i](config)
	}
}

func WithOutputDir(outputDir string) ConfigurationOption {
	return func(conf *Configuration) {
		conf.outputDir = outputDir
	}
}

func WithServiceName(serviceName string) ConfigurationOption {
	return func(conf *Configuration) {
		config.Command.Use = serviceName
	}
}

func WithProjectRoot(rootDir string) ConfigurationOption {
	_, filename, _, _ := runtime.Caller(1)

	return func(conf *Configuration) {
		config.projectRoot = filepath.Join(filepath.Dir(filename), rootDir)
	}
}

func AddCommand(cmds ...*cobra.Command) {
	config.Command.AddCommand(cmds...)
}

func Execute(run func(cmd *cobra.Command, args []string)) {
	config.Command.Run = run
	if err := config.Execute(); err != nil {
		panic(err)
	}
}

func ReflectConf(c interface{}) {
	tpe := reflect.TypeOf(c)
	if tpe.Kind() != reflect.Ptr {
		panic(fmt.Errorf("ReflectConf pass ptr for setting value"))
	}

	os.Setenv("PROJECT_NAME", config.ProjectName())

	config.scanDefaultVars(c)
	config.scanEnv(c)

	config.log(c)

	triggerInitials(c)
}

func triggerInitials(c interface{}) {
	rv := reflectx.Indirect(reflect.ValueOf(c))
	for i := 0; i < rv.NumField(); i++ {
		value := rv.Field(i)
		if conf, ok := value.Interface().(interface{ Init() error }); ok {
			if err := conf.Init(); err != nil {
				panic(err)
			}
		}
	}
}

type Configuration struct {
	*cobra.Command
	Feature              string
	outputDir            string
	projectRoot          string
	ShouldGenerateConfig bool
	defaultEnvVars       envconf.EnvVars
}

func (conf *Configuration) ProjectName() string {
	if conf.Feature != "" {
		return conf.ServiceName() + "--" + conf.Feature
	}
	return conf.ServiceName()
}

func (conf *Configuration) ServiceName() string {
	return conf.Use
}

func (conf *Configuration) Prefix() string {
	return strings.ToUpper(strings.Replace(conf.Use, "-", "_", -1))
}

func (conf *Configuration) init() {
	if projectFeature, exists := os.LookupEnv("PROJECT_FEATURE"); exists {
		conf.Feature = projectFeature
	}

	conf.Command = &cobra.Command{
		PreRun: func(cmd *cobra.Command, args []string) {
			if conf.ShouldGenerateConfig {
				conf.generateYaml()
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
		},
	}

	if conf.Use == "" {
		conf.Use = "srv-x"
	}

	conf.PersistentFlags().
		BoolVarP(&conf.ShouldGenerateConfig, "output-docker-config", "c", true, "output configuration of docker")
}

func (conf *Configuration) scanDefaultVars(c interface{}) {
	if err := envconf.NewDotEnvDecoder(&conf.defaultEnvVars).Decode(c); err != nil {
		panic(err)
	}
	if _, err := envconf.NewDotEnvEncoder(&conf.defaultEnvVars).Encode(c); err != nil {
		panic(err)
	}
	conf.scanFromLocal(c)
}

func (conf *Configuration) log(c interface{}) {
	envVars := envconf.NewEnvVars(conf.Prefix())
	if _, err := envconf.NewDotEnvEncoder(envVars).Encode(c); err != nil {
		panic(err)
	}
	fmt.Printf("%s", string(envVars.MaskBytes()))
}

func (conf *Configuration) scanFromLocal(c interface{}) {
	contents, err := ioutil.ReadFile(filepath.Join(conf.projectRoot, "./config/local.yml"))
	if err != nil {
		return
	}
	keyValues := map[string]string{}
	err = yaml.Unmarshal(contents, &keyValues)
	if err != nil {
		fmt.Printf("parse error: %s", err)
		return
	}

	envVars := &envconf.EnvVars{
		Prefix: conf.Prefix(),
	}
	for key, value := range keyValues {
		envVars.SetKeyValue(key, value)
	}
	if err := envconf.NewDotEnvDecoder(envVars).Decode(c); err != nil {
		panic(err)
	}
}

func (conf *Configuration) scanEnv(c interface{}) {
	envVars := envconf.EnvVarsFromEnviron(conf.Prefix(), os.Environ())
	if err := envconf.NewDotEnvDecoder(envVars).Decode(c); err != nil {
		panic(err)
	}
}

func (conf *Configuration) generateYaml() {
	writeToYamlFile("./config/default.yml", conf.defaultConfig(), yaml.Marshal)
}

func (conf *Configuration) defaultConfig() map[string]string {
	m := map[string]string{}
	m["GOENV"] = "DEV"

	for _, envVar := range conf.defaultEnvVars.Values {
		if !envVar.Optional {
			m[envVar.Key(conf.Prefix())] = envVar.Value
		}
	}

	return m
}

func writeToYamlFile(filename string, v interface{}, marshal func(v interface{}) ([]byte, error)) error {
	bytes, _ := marshal(v)
	dir := filepath.Dir(filename)
	if dir != "" {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return ioutil.WriteFile(filename, bytes, os.ModePerm)
}
