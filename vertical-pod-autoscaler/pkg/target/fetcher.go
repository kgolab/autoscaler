/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package target

import (
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1beta2"
	"k8s.io/client-go/discovery"
	cacheddiscovery "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/dynamic"
	appsinformer "k8s.io/client-go/informers/apps/v1"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"

	"github.com/golang/glog"
)

const (
	resyncPeriod time.Duration = 1 * time.Minute
	discoveryResetPeriod time.Duration = 5 * time.Minute
)

// VpaTargetSelectorFetcher gets a labelSelector used to gather Pods controlled by the given VPA.
type VpaTargetSelectorFetcher interface {
	// Fetch returns a labelSelector used to gather Pods controlled by the given VPA.
	// If error is nil, the returned labelSelector is not nil.
	Fetch(vpa *vpa_types.VerticalPodAutoscaler) (labels.Selector, error)
}

// NewVpaTargetSelectorFetcher returns new instance of VpaTargetSelectorFetcher
func NewVpaTargetSelectorFetcher(config *rest.Config, kubeClient kube_client.Interface) VpaTargetSelectorFetcher {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		glog.Fatalf("Could not create discoveryClient: %v", err)
	}
	resolver := scale.NewDiscoveryScaleKindResolver(discoveryClient)
	restClient := kubeClient.CoreV1().RESTClient()
	cachedDiscoveryClient := cacheddiscovery.NewMemCacheClient(discoveryClient)
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedDiscoveryClient)
	go wait.Until(func() {
		mapper.Reset()
	}, discoveryResetPeriod, make(chan struct{}))

	informer := appsinformer.NewDaemonSetInformer(kubeClient, apiv1.NamespaceAll,
		resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	stopCh := make(chan struct{})
	go informer.Run(stopCh)
	synced := cache.WaitForCacheSync(stopCh, informer.HasSynced)
	if !synced {
		glog.Fatalf("Could not sync cache for DaemonSets: %v", err)
	}

	scaleNamespacer := scale.New(restClient, mapper, dynamic.LegacyAPIPathResolverFunc, resolver)
	return &vpaTargetSelectorFetcher{
		scaleNamespacer: scaleNamespacer,
		mapper:          mapper,
		daemonInformer:  informer,
	}
}

// vpaTargetSelectorFetcher implements VpaTargetSelectorFetcher interface
// by querying API server for the controller pointed by VPA's targetRef
type vpaTargetSelectorFetcher struct {
	scaleNamespacer scale.ScalesGetter
	mapper          apimeta.RESTMapper
	daemonInformer  cache.SharedIndexInformer
}

func (f *vpaTargetSelectorFetcher) Fetch(vpa *vpa_types.VerticalPodAutoscaler) (labels.Selector, error) {
	// treat DaemonSet differently, it does not support scale subresource
	if vpa.Spec.TargetRef.Kind == "DaemonSet" {
		return f.getLabelSelectorForDaemonSet(vpa.Namespace, vpa.Spec.TargetRef.Name)
	}

	// not on a list of known controllers, use scale sub-resource
	groupVersion, err := schema.ParseGroupVersion(vpa.Spec.TargetRef.APIVersion)
	if err != nil {
		return nil, err
	}
	groupKind := schema.GroupKind{
		Group: groupVersion.Group,
		Kind:  vpa.Spec.TargetRef.Kind,
	}

	selector, err := f.getLabelSelectorFromResource(groupKind, vpa.Namespace, vpa.Spec.TargetRef.Name)
	if err != nil {
		return nil, fmt.Errorf("Unhandled TargetRef %s / %s / %s, last error %v",
			vpa.Spec.TargetRef.APIVersion, vpa.Spec.TargetRef.Kind, vpa.Spec.TargetRef.Name, err)
	}
	return selector, nil
}

func (f *vpaTargetSelectorFetcher) getLabelSelectorForDaemonSet(namespace, name string) (labels.Selector, error) {
	daemonObj, exists, err := f.daemonInformer.GetStore().GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if ! exists {
		return nil, fmt.Errorf("DaemonSet %s/%s does not exist", namespace, name)
	}

	daemon, ok := daemonObj.(*appsv1.DaemonSet)
	if ! ok {
		return nil, fmt.Errorf("Failed to parse DaemonSet %s/%s", namespace, name)
	}

	return metav1.LabelSelectorAsSelector(daemon.Spec.Selector)
}

func (f *vpaTargetSelectorFetcher) getLabelSelectorFromResource(
	groupKind schema.GroupKind, namespace, name string,
) (labels.Selector, error) {
	mappings, err := f.mapper.RESTMappings(groupKind)
	if err != nil {
		return nil, err
	}

	var lastError error
	for _, mapping := range mappings {
		groupResource := mapping.Resource.GroupResource()
		scale, err := f.scaleNamespacer.Scales(namespace).Get(groupResource, name)
		if err == nil {
			selector, err := labels.Parse(scale.Status.Selector)
			if err != nil {
				return nil, err
			}
			return selector, nil
		} else {
			lastError = err
		}
	}

	// nothing found, apparently the resource does support scale (or we lack RBAC)
	return nil, lastError
}
