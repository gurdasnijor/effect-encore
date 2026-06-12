// Shared actor-kind brand so `Actor.isEntity` / `Actor.isWorkflow` can
// discriminate an entity actor from a workflow actor at runtime (both are plain
// objects). Keyed on a registered symbol so the tag survives across module
// boundaries (entity branded in actor.ts, workflow in workflow.ts).

export const ActorKindId: unique symbol = Symbol.for("encore-ds/ActorKind");

export type ActorKind = "entity" | "workflow";

export interface HasActorKind {
  readonly [ActorKindId]: ActorKind;
}

export const kindOf = (value: unknown): ActorKind | undefined =>
  typeof value === "object" && value !== null
    ? (value as Partial<HasActorKind>)[ActorKindId]
    : undefined;
