package org.example;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.util.Objects;

public class AvatarBook {

    private String title;
    private String description;

    public AvatarBook() {
    }

    public AvatarBook(String title, String description) {
        this.title = title;
        this.description = description;
    }


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public static Encoder<AvatarBook> getEncoder() {
        return Encoders.bean(AvatarBook.class);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        var avatarBook = (AvatarBook) obj;
        return Objects.equals(getTitle(), avatarBook.getTitle()) && Objects.equals(getDescription(), avatarBook.getDescription());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTitle(), getDescription());
    }

    @Override
    public String toString() {
        return "AvatarBook{" +
                "title=" + title +
                ", description=" + description +
                "}";
    }
}
