import { Assignment } from "../api/sync-group";
import { Metadata } from "../metadata";

export class ConsumerMetadata extends Metadata {
    private assignment: Assignment = {};

    public getAssignment() {
        return this.assignment;
    }

    public setAssignment(newAssignment: Assignment) {
        this.assignment = newAssignment;
    }
}
