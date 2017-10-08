package fr.cvillard.jet;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * A Customer has an ID and a name
 */
public class Customer implements DataSerializable {
	private int id;

	private String name;

	public Customer() {
	}

	public Customer(int id, String name) {
		this.id = id;
		this.name = name;
	}

	/**
	 * @return id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id Value of id
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name Value of name
	 */
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		id = in.readInt();
		name = in.readUTF();
	}

	@Override
	public String toString() {
		return "Customer{" +
				"id=" + id +
				", name='" + name + '\'' +
				'}';
	}
}
