package function

import (
	"context"
	"encoding/base64"
	"fmt"
	fv1 "github.com/fission/fission/pkg/apis/core/v1"
	ferror "github.com/fission/fission/pkg/error"
	"github.com/fission/fission/pkg/fission-cli/cliwrapper/cli"
	"github.com/fission/fission/pkg/fission-cli/cmd"
	"github.com/fission/fission/pkg/fission-cli/cmd/spec"
	"github.com/fission/fission/pkg/fission-cli/console"
	flagkey "github.com/fission/fission/pkg/fission-cli/flag/key"
	"github.com/fission/fission/pkg/fission-cli/util"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"os"
	"strings"
	"time"
)

type RunKuasarWasmSubCommand struct {
	cmd.CommandActioner
	function *fv1.Function
	specFile string
}

func getK8sClient() *kubernetes.Clientset {
	config, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		log.Fatalf("failed to create kubeconfig: %v", err)
	}
	fmt.Println("Kubeconfig loaded successfully.")
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("failed to create Kubernetes client: %v", err)
	}
	fmt.Println("Kubernetes client created.")
	return clientset
}

func RunKuasarWasm(input cli.Input) error {
	return (&RunKuasarWasmSubCommand{}).do(input)
}

func (opts *RunKuasarWasmSubCommand) do(input cli.Input) error {
	err := opts.complete(input)
	if err != nil {
		return err
	}
	return opts.run(input)
}

func (opts *RunKuasarWasmSubCommand) complete(input cli.Input) error {
	fnName := input.String(flagkey.FnName)
	fnNamespace := input.String(flagkey.NamespaceFunction)

	// user wants a spec, create a yaml file with package and function
	toSpec := false
	if input.Bool(flagkey.SpecSave) {
		toSpec = true
		opts.specFile = fmt.Sprintf("function-%v.yaml", fnName)
	}

	if !toSpec {
		// check for unique function names within a namespace
		fn, err := opts.Client().V1().Function().Get(&metav1.ObjectMeta{
			Name:      input.String(flagkey.FnName),
			Namespace: input.String(flagkey.NamespaceFunction),
		})
		if err != nil && !ferror.IsNotFound(err) {
			return err
		} else if fn != nil {
			return errors.New("a function with the same name already exists")
		}
	}

	fnTimeout := input.Int(flagkey.FnExecutionTimeout)
	if fnTimeout <= 0 {
		return errors.Errorf("--%v must be greater than 0", flagkey.FnExecutionTimeout)
	}

	fnIdleTimeout := input.Int(flagkey.FnIdleTimeout)

	secretNames := input.StringSlice(flagkey.FnSecret)
	cfgMapNames := input.StringSlice(flagkey.FnCfgMap)

	es, err := getExecutionStrategy(fv1.ExecutorTypeWasm, input)
	if err != nil {
		return err
	}
	invokeStrategy := &fv1.InvokeStrategy{
		ExecutionStrategy: *es,
		StrategyType:      fv1.StrategyTypeExecution,
	}
	resourceReq, err := util.GetResourceReqs(input, &apiv1.ResourceRequirements{})
	if err != nil {
		return err
	}

	fnGracePeriod := input.Int64(flagkey.FnGracePeriod)
	if fnGracePeriod < 0 {
		console.Warn("grace period must be a non-negative integer, using default value (6 mins)")
	}

	var imageName string
	var port int
	var command, args string

	if input.IsSet(flagkey.FnSubmitWasmImage) {
		imageName = input.String(flagkey.FnSubmitWasmImage)
	} else if input.IsSet(flagkey.FnSubmitWasmBinary) {
		wasmFileName := input.String(flagkey.FnSubmitWasmBinary)
		buildID := uuid.NewString()
		wasmData, err := os.ReadFile("./" + wasmFileName)
		wasmEnvName := input.String(flagkey.FnName) + "-wasmfunction"
		if err != nil {
			return fmt.Errorf("loading wasmdata failed: %w", err)
		}
		imageUrl, err := opts.kanikoBuildJob(input, buildID, wasmFileName, wasmData, wasmEnvName)
		if err != nil {
			console.Error("❌ Build function image failed, check Kaniko logs for details")
			return fmt.Errorf("function image build failed: %w", err)
		}
		imageName = imageUrl
	}

	port = input.Int(flagkey.FnPort)
	command = input.String(flagkey.FnCommand)
	args = input.String(flagkey.FnArgs)

	var secrets []fv1.SecretReference
	var cfgmaps []fv1.ConfigMapReference

	if len(secretNames) > 0 {
		// check the referenced secret is in the same ns as the function, if not give a warning.
		if !toSpec { // TODO: workaround in order not to block users from creating function spec, remove it.
			for _, secretName := range secretNames {
				err := opts.Client().V1().Misc().SecretExists(&metav1.ObjectMeta{
					Namespace: fnNamespace,
					Name:      secretName,
				})
				if err != nil {
					if k8serrors.IsNotFound(err) {
						console.Warn(fmt.Sprintf("Secret %s not found in Namespace: %s. Secret needs to be present in the same namespace as function", secretName, fnNamespace))
					} else {
						return errors.Wrapf(err, "error checking secret %s", secretName)
					}
				}
			}
		}
		for _, secretName := range secretNames {
			newSecret := fv1.SecretReference{
				Name:      secretName,
				Namespace: fnNamespace,
			}
			secrets = append(secrets, newSecret)
		}
	}

	if len(cfgMapNames) > 0 {
		// check the referenced cfgmap is in the same ns as the function, if not give a warning.
		if !toSpec {
			for _, cfgMapName := range cfgMapNames {
				err := opts.Client().V1().Misc().ConfigMapExists(&metav1.ObjectMeta{
					Namespace: fnNamespace,
					Name:      cfgMapName,
				})
				if err != nil {
					if k8serrors.IsNotFound(err) {
						console.Warn(fmt.Sprintf("ConfigMap %s not found in Namespace: %s. ConfigMap needs to be present in the same namespace as function", cfgMapName, fnNamespace))
					} else {
						return errors.Wrapf(err, "error checking configmap %s", cfgMapName)
					}
				}
			}
		}
		for _, cfgMapName := range cfgMapNames {
			newCfgMap := fv1.ConfigMapReference{
				Name:      cfgMapName,
				Namespace: fnNamespace,
			}
			cfgmaps = append(cfgmaps, newCfgMap)
		}
	}

	opts.function = &fv1.Function{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fnName,
			Namespace: fnNamespace,
		},
		Spec: fv1.FunctionSpec{
			Secrets:         secrets,
			ConfigMaps:      cfgmaps,
			Resources:       *resourceReq,
			InvokeStrategy:  *invokeStrategy,
			FunctionTimeout: fnTimeout,
			IdleTimeout:     &fnIdleTimeout,
		},
	}

	err = util.ApplyLabelsAndAnnotations(input, &opts.function.ObjectMeta)
	if err != nil {
		return err
	}

	container := &apiv1.Container{
		Name:  fnName,
		Image: imageName,
		Ports: []apiv1.ContainerPort{
			{
				Name:          "http-env",
				ContainerPort: int32(port),
			},
		},
	}

	if command != "" {
		container.Command = strings.Split(command, ",")
	}
	if args != "" {
		container.Args = strings.Split(args, " ")
	}

	opts.function.Spec.PodSpec = &apiv1.PodSpec{
		Containers:                    []apiv1.Container{*container},
		TerminationGracePeriodSeconds: &fnGracePeriod,
	}

	return nil
}

// run write the resource to a spec file or create a fission CRD with remote fission server.
// It also prints warning/error if necessary.
func (opts *RunKuasarWasmSubCommand) run(input cli.Input) error {
	// if we're writing a spec, don't create the function
	// save to spec file or display the spec to console
	if input.Bool(flagkey.SpecDry) {
		return spec.SpecDry(*opts.function)
	}

	if input.Bool(flagkey.SpecSave) {
		err := spec.SpecSave(*opts.function, opts.specFile)
		if err != nil {
			return errors.Wrap(err, "error saving function spec")
		}
		return nil
	}

	_, err := opts.Client().V1().Function().Create(opts.function)
	if err != nil {
		return errors.Wrap(err, "error creating function")
	}

	fmt.Printf("function '%v' created\n", opts.function.ObjectMeta.Name)
	return nil
}

func (opts *RunKuasarWasmSubCommand) kanikoBuildJob(input cli.Input, buildID string, wasmFileName string, wasmData []byte, imageName string) (string, error) {
	client := getK8sClient()
	namespace := "fission"
	jobName := "kaniko-job-" + buildID
	imageUrl := fmt.Sprintf("registry.fission.svc.cluster.local:5000/%s:latest", imageName)
	contextPath := fmt.Sprintf("/mnt/kaniko-context/%s", buildID)
	dockerfilePath := fmt.Sprintf("%s/Dockerfile", contextPath)

	dockerfile := fmt.Sprintf(`FROM scratch
COPY %s /
CMD ["\"/%s\""]
`, wasmFileName, wasmFileName)

	wasmBase64 := base64.StdEncoding.EncodeToString(wasmData)

	script := fmt.Sprintf(`
mkdir -p /mnt/kaniko-context/%s && \
echo "%s" > /mnt/kaniko-context/%s/Dockerfile && \
echo "%s" | base64 -d > /mnt/kaniko-context/%s/%s
`, buildID, dockerfile, buildID, wasmBase64, buildID, wasmFileName)

	fmt.Printf("Creating job %s in namespace %s\n", jobName, namespace)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: int32Ptr(60),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					NodeSelector: map[string]string{
						"kaniko-builder": "true",
					},
					InitContainers: []corev1.Container{
						{
							Name:    "prep",
							Image:   "alpine",
							Command: []string{"sh", "-c"},
							Args:    []string{script},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "context", MountPath: "/mnt/kaniko-context"},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "kaniko",
							Image: "gcr.io/kaniko-project/executor:latest",
							Args: []string{
								fmt.Sprintf("--context=dir://%s", contextPath),
								fmt.Sprintf("--dockerfile=%s", dockerfilePath),
								fmt.Sprintf("--destination=%s", imageUrl),
								"--insecure",
								"--skip-tls-verify",
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "context", MountPath: "/mnt/kaniko-context"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "context",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/mnt/kaniko-context",
									Type: newHostPathType("Directory"),
								},
							},
						},
					},
				},
			},
		},
	}

	fmt.Printf("Sending request to create job %s\n", jobName)

	_, err := client.BatchV1().Jobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create kaniko job failed: %w", err)
	}

	maxRetries := int32(3)
	timeout := time.After(3 * time.Minute)

	for {
		select {
		case <-timeout:
			_ = client.BatchV1().Jobs(namespace).Delete(context.TODO(), jobName, metav1.DeleteOptions{})
			return "", fmt.Errorf("kaniko job timeout after 3 minutes")
		default:
			time.Sleep(2 * time.Second)
			j, err := client.BatchV1().Jobs(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			if j.Status.Succeeded > 0 {
				goto SUCCESS
			}
			if j.Status.Failed >= maxRetries {
				logs, _ := getJobLogs(client, jobName, namespace)
				fmt.Println("🔥 Kaniko Job failed. Logs:")
				fmt.Println(logs)
				_ = client.BatchV1().Jobs(namespace).Delete(context.TODO(), jobName, metav1.DeleteOptions{})
				return "", fmt.Errorf("kaniko job failed after %d retries", j.Status.Failed)
			}
			fmt.Println("⏳ Building image with Kaniko...")
		}
	}

SUCCESS:
	fmt.Println("✅ Image built and pushed:", imageUrl)
	//_ = os.RemoveAll(fmt.Sprintf("/mnt/kaniko-context/%s", buildID))
	return imageUrl, nil
}

func getJobLogs(client *kubernetes.Clientset, jobName, namespace string) (string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil || len(pods.Items) == 0 {
		return "", fmt.Errorf("cannot find pod for job %s", jobName)
	}
	podName := pods.Items[0].Name
	logs, err := client.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "kaniko",
	}).Do(context.TODO()).Raw()
	if err != nil {
		return "", err
	}
	return string(logs), nil
}

func newHostPathType(pathType string) *corev1.HostPathType {
	t := corev1.HostPathType(pathType)
	return &t
}

func int32Ptr(i int32) *int32 { return &i }
