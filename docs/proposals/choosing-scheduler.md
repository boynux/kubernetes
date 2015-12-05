<!-- BEGIN MUNGE: UNVERSIONED_WARNING -->

<!-- BEGIN STRIP_FOR_RELEASE -->

<img src="http://kubernetes.io/img/warning.png" alt="WARNING"
     width="25" height="25">
<img src="http://kubernetes.io/img/warning.png" alt="WARNING"
     width="25" height="25">
<img src="http://kubernetes.io/img/warning.png" alt="WARNING"
     width="25" height="25">
<img src="http://kubernetes.io/img/warning.png" alt="WARNING"
     width="25" height="25">
<img src="http://kubernetes.io/img/warning.png" alt="WARNING"
     width="25" height="25">

<h2>PLEASE NOTE: This document applies to the HEAD of the source tree</h2>

If you are using a released version of Kubernetes, you should
refer to the docs that go with that version.

<strong>
The latest release of this document can be found
[here](http://releases.k8s.io/release-1.1/docs/proposals/choosing-scheduler.md).

Documentation for other releases can be found at
[releases.k8s.io](http://releases.k8s.io).
</strong>
--

<!-- END STRIP_FOR_RELEASE -->

<!-- END MUNGE: UNVERSIONED_WARNING -->

# Choosing scheduler in a multi-scheduler configuration

## Introduction

This document proposes a mechanism for assigning a scheduler to a pod
in a system which is using multiple schedulers as described [here](multiple-schedulers.md).
In particular, we decribe an admission controller which adds the scheduler name annotation,
and which takes its configuration from a new API object called `PodPolicy` that determines,
for each pod, which scheduler is responsible for sheduling that pod, based on various
characteristics of the pod.

## Implementation

We will create a new admission controller called PodSchedulerPolicy. It will be configured
using a new API object called `PodPolicy`. We have chosen a generic name and API
for this object because we want to use the same object for defining other policies based on pod
characteristics, for example see #17097.

PodSchedulerPolicy will iterate through the `Rules` of the `PodPolicy` and apply the
first one that matches the pod. Each `Rule` is a string specifying the scheduler name
to apply to the pod if the pod satisfies the rule's PolicyPredicate, which currently is
just a wrapper over `PodSelector`.

## API

### PodPolicy

```go
// A PodPolicy is a list of rules that define the policy. Each rule is tested
// in order, and the first one that matches is applied.
type PodPolicy struct {
	Rules []PodPolicyRule `json:"rules,omitempty"`
}

type PodPolicyRule struct {
	// The rule defined by Policy is applied if the pod satisfies all of the criteria in PolicyPredicate
	// (currently this just means satisfying the PodSelector in PolicyPredicate).
	// The reason we use PolicyPredicate instead of just putting *PodSelector here is to allow
	// us to cleanly add additional predicates in the future; they would be added to
	// PolicyPredicate, allowing PodPolicyRule to stay simple.
	PolicyPredicate *PolicyPredicate `json:"policyPredicate,omitempty"`
	Policy string `json:"policy"`
}

// Note that Namespace is not part of PolicyPredicate because we assume pod policies are per-Namespace.
type PolicyPredicate struct {
	PodSelector *PodSelector `json:"podSelector,omitempty"`
}
```

### PodSchedulerPolicy admission controller

```go
// PodSchedulerPolicySpec defines the policy configuration for the PodSchedulerPolicy admission controller.
type PodSchedulerPolicySpec struct {
	Policy PodPolicy `json:"policy,omitempty"`
}

// PodSchedulerPolicyStatus just reflects back the policy set in the spec.
type PodSchedulerPolicyStatus struct {
	Policy PodPolicy `json:"policy,omitempty"`
}

// PodSchedulerPolicy describes the PodSchedulerPolicy admission controller,
// which maps pods to the scheduler responsible for scheduling them.
type PodSchedulerPolicy struct {
	unversioned.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#metadata
	ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the pod scheduler policy implemented by this admission controller.
	// http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#spec-and-status
	Spec PodSchedulerPolicySpec `json:"spec,omitempty"`

	// Status reflects back the spec.
	// http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#spec-and-status
	Status PodSchedulerPolicyStatus `json:"status,omitempty"`
}

// PodSchedulerPolicyList is a list of PodSchedulerPolicy items.
type PodSchedulerPolicyList struct {
	unversioned.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: http://releases.k8s.io/HEAD/docs/devel/api-conventions.md#types-kinds
	unversioned.ListMeta `json:"metadata,omitempty"`

	// Items is a list of PodSchedulerPolicy objects.
	// More info: http://releases.k8s.io/HEAD/docs/design/admission_control_resource_quota.md#admissioncontrol-plugin-resourcequota
	Items []PodSchedulerPolicy `json:"items"`
}
```

## Implementation plan

1. Create the `PodPolicy` API resource
1. Create the PodSchedulerPolicy admission controller

## Future work

In the future we may add more pod properties to `PolicyPredicate`. We considered allowing
`PolicyPredicate` to look at pod resource requirements and limits, so it could direct pods to
schedulers based on their QoS class (best-effort, burstable, guaranteed). We might do this in
the future, but for now we will have the admission controller set a label
`scheduler.alpha.kubernetes.io/qos: burstable` on all pods that do not have a label with
that key, and assume that users will set that label themselves on best-effort and guaranteed pods.
Then PodSchedulerPolicy can implement a policy that chooses the scheduler based on QoS class
just using a PodSelector.

We plan to eventually move from having an admission controller
set the scheduler name as a pod annotation, to using the initializer concept. In particular, the
scheduler will be an initializer, and the admission controller that decides which scheduler to use
will add the scheduler's name to the list of initializers for the pod (presumably the scheduler
will be the last initializer to run on each pod).
The admission controller would still be configured using the `PodPolicy` described here, only the
mechanism the admission controller uses to record its decision of which scheduler to use would change.

## Related issues

The main issue for multiple schedulers is #11793. There was also a lot of discussion
in PRs #17197 and #17865. Issue #17097 describes a scenario unrelated to scheduler-choosing
where `PodPolicy` could be used. Issue #17324 proposes to create a generalized API for matching
"claims" to "service classes"; matching a pod to a scheduler would be one use for such an API.


<!-- BEGIN MUNGE: GENERATED_ANALYTICS -->
[![Analytics](https://kubernetes-site.appspot.com/UA-36037335-10/GitHub/docs/proposals/choosing-scheduler.md?pixel)]()
<!-- END MUNGE: GENERATED_ANALYTICS -->
