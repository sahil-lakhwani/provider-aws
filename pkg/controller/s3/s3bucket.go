/*
Copyright 2019 The Crossplane Authors.

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

package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/crossplane/provider-aws/apis/storage/v1beta1"
	bucketv1beta1 "github.com/crossplane/provider-aws/apis/storage/v1beta1"
	"github.com/crossplane/provider-aws/pkg/clients/s3"

	runtimev1alpha1 "github.com/crossplane/crossplane-runtime/apis/core/v1alpha1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	awsv1alpha3 "github.com/crossplane/provider-aws/apis/v1alpha3"
	awsclients "github.com/crossplane/provider-aws/pkg/clients"
)

const (
	controllerName = "s3bucket.aws.crossplane.io"
	finalizer      = "finalizer." + controllerName
)

// Amounts of time we wait before requeuing a reconcile.
const (
	aLongWait = 60 * time.Second
)

// Error strings
const (
	errUpdateManagedStatus = "cannot update managed resource status"
	errUnexpectedObject    = "The managed resource is not a S3Bucket resource"
	errClient              = "cannot create a new S3 client"

	errCreates3Client = "cannot create RDS client"
	errCreateBucket   = "cannot create S3 bucket"
	errGetBucket      = "cannot get S3 bucket"

	errGetProvider       = "cannot get provider"
	errGetProviderSecret = "cannot get provider secret"
)

var (
	ctx           = context.Background()
	result        = reconcile.Result{}
	resultRequeue = reconcile.Result{Requeue: true}
)

// Reconciler reconciles a S3Bucket object
type Reconciler struct {
	client.Client
	scheme *runtime.Scheme
	managed.ReferenceResolver
	managed.ConnectionPublisher

	connect func(*bucketv1beta1.S3Bucket) (s3.Service, error)
	create  func(*bucketv1beta1.S3Bucket, s3.Service) (reconcile.Result, error)
	sync    func(*bucketv1beta1.S3Bucket, s3.Service) (reconcile.Result, error)
	delete  func(*bucketv1beta1.S3Bucket, s3.Service) (reconcile.Result, error)

	log logging.Logger
}

// SetupS3Bucket adds a controller that reconciles S3Buckets.
func SetupS3Bucket(mgr ctrl.Manager, l logging.Logger) error {
	name := managed.ControllerName(bucketv1beta1.S3BucketClassGroupKind)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&bucketv1beta1.S3Bucket{}).
		Owns(&corev1.Secret{}).
		Complete(managed.NewReconciler(mgr,
			resource.ManagedKind(bucketv1beta1.S3BucketGroupVersionKind),
			managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), newClientFn: s3.NewClient}),
			managed.WithLogger(l.WithValues("controller", name)),
			managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name)))))
}

type connector struct {
	kube        client.Client
	newClientFn func(ctx context.Context, credentials []byte, region string, auth awsclients.AuthMethod) (s3.Client, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*bucketv1beta1.S3Bucket)
	if !ok {
		return nil, errors.New(errUnexpectedObject)
	}

	p := &awsv1alpha3.Provider{}
	if err := c.kube.Get(ctx, meta.NamespacedNameOf(cr.Spec.ProviderReference), p); err != nil {
		return nil, errors.Wrap(err, errGetProvider)
	}

	if aws.BoolValue(p.Spec.UseServiceAccount) {
		s3Client, err := c.newClientFn(ctx, []byte{}, p.Spec.Region, awsclients.UsePodServiceAccount)
		return &external{client: s3Client, kube: c.kube}, errors.Wrap(err, errCreates3Client)
	}

	if p.GetCredentialsSecretReference() == nil {
		return nil, errors.New(errGetProviderSecret)
	}

	s := &corev1.Secret{}
	n := types.NamespacedName{Namespace: p.Spec.CredentialsSecretRef.Namespace, Name: p.Spec.CredentialsSecretRef.Name}
	if err := c.kube.Get(ctx, n, s); err != nil {
		return nil, errors.Wrap(err, errGetProviderSecret)
	}

	s3Client, err := c.newClientFn(ctx, s.Data[p.Spec.CredentialsSecretRef.Key], p.Spec.Region, awsclients.UseProviderSecret)
	return &external{client: s3Client, kube: c.kube}, errors.Wrap(err, errCreates3Client)
}

type external struct {
	client s3.Client
	kube   client.Client
}

func (e *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1beta1.S3Bucket)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errUnexpectedObject)
	}

	// List buckets and check if bucket in spec exists
	result, err := e.client.ListBucketsRequest(&awsS3.ListBucketsInput{}).Send(ctx)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errGetBucket)
	}

	var bucket *awsS3.Bucket
	for i := 0; i < len(result.Buckets); i++ {
		if *result.Buckets[i].Name == meta.GetExternalName(cr) {
			bucket = &result.Buckets[i]
		}
	}

	if bucket == nil {
		return managed.ExternalObservation{
			ResourceExists: false,
		}, nil
	}

	return managed.ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: true,
	}, nil
}

func (e *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*bucketv1beta1.S3Bucket)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errUnexpectedObject)
	}
	cr.SetConditions(runtimev1alpha1.Creating())

	// Create the bucket
	_, err := e.client.CreateBucketRequest(s3.CreateBucketInput(meta.GetExternalName(cr), &cr.Spec.ForProvider)).Send(ctx)

	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateBucket)
	}

	// Attach the policy
	_, err = e.client.PutBucketPolicyRequest(&awsS3.PutBucketPolicyInput{
		Bucket: aws.String(meta.GetExternalName(cr)),
		Policy: cr.Spec.ForProvider.Policy,
	}).Send(ctx)

	return managed.ExternalCreation{}, nil
}

func (e *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, nil
}

func (e *external) Delete(ctx context.Context, mg resource.Managed) error {
	return nil
}
